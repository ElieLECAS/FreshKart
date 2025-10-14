#!/usr/bin/env python3
"""
Script d'import des données FreshKart vers PostgreSQL
Parse les fichiers CSV et JSON et les stocke dans une base PostgreSQL
"""

import os
import json
import glob
import re
import shutil
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Boolean, ForeignKey, and_, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from datetime import date, timedelta

# Configuration de la base de données
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/freshkart')

# Configuration des chemins
DATA_INPUT_PATH = '/data/input'
TEMP_PATH = '/app/temp'
OUTPUT_PATH = '/app/output'

# Configuration de la date cible
TARGET_DATE_ENV = os.getenv('TARGET_DATE')
if TARGET_DATE_ENV:
    TARGET_DATE = date.fromisoformat(TARGET_DATE_ENV)
else:
    TARGET_DATE = date.today() - timedelta(days=1)

# Initialisation SQLAlchemy
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Définition des modèles SQLAlchemy
class Customer(Base):
    __tablename__ = 'customers'
    
    customer_id = Column(String(10), primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=False)
    city = Column(String(100), nullable=False)
    is_active = Column(Boolean, nullable=False)
    
    # Relation avec les commandes
    orders = relationship("Order", back_populates="customer")

class Order(Base):
    __tablename__ = 'orders'
    
    order_id = Column(String(20), primary_key=True)
    customer_id = Column(String(10), ForeignKey('customers.customer_id'), nullable=False)
    channel = Column(String(20), nullable=False)
    created_at = Column(DateTime, nullable=False)
    payment_status = Column(String(20), nullable=False)
    
    # Relations
    customer = relationship("Customer", back_populates="orders")
    items = relationship("OrderItem", back_populates="order")
    refunds = relationship("Refund", back_populates="order")

class OrderItem(Base):
    __tablename__ = 'order_items'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(20), ForeignKey('orders.order_id'), nullable=False)
    sku = Column(String(50), nullable=False)
    qty = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    
    # Relation
    order = relationship("Order", back_populates="items")

class Refund(Base):
    __tablename__ = 'refunds'
    
    refund_id = Column(String(20), primary_key=True)
    order_id = Column(String(20), ForeignKey('orders.order_id'), nullable=False)
    amount = Column(Float, nullable=False)
    reason = Column(String(100), nullable=False)
    created_at = Column(DateTime, nullable=False)
    
    # Relation
    order = relationship("Order", back_populates="refunds")

class OrderClean(Base):
    """Table orders_clean - Agrégation des commandes nettoyées"""
    __tablename__ = 'orders_clean'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(20), nullable=False)
    customer_id = Column(String(10), nullable=False)
    channel = Column(String(20), nullable=False)
    city = Column(String(100), nullable=False)
    order_date = Column(DateTime, nullable=False)
    items_sold = Column(Integer, nullable=False)
    gross_revenue_eur = Column(Float, nullable=False)
    refunds_eur = Column(Float, nullable=False, default=0.0)
    net_revenue_eur = Column(Float, nullable=False)

class DailyCitySales(Base):
    """Table daily_city_sales - Résumé quotidien par ville et canal"""
    __tablename__ = 'daily_city_sales'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False)
    city = Column(String(100), nullable=False)
    channel = Column(String(20), nullable=False)
    orders_count = Column(Integer, nullable=False)
    unique_customers = Column(Integer, nullable=False)
    items_sold = Column(Integer, nullable=False)
    gross_revenue_eur = Column(Float, nullable=False)
    refunds_eur = Column(Float, nullable=False)
    net_revenue_eur = Column(Float, nullable=False)

def create_tables():
    """Crée toutes les tables si elles n'existent pas"""
    print("🏗️  Création des tables...")
    Base.metadata.create_all(engine)
    print("✅ Tables créées/vérifiées")

def copy_files_to_temp():
    """Copie tous les fichiers de data/input vers le dossier temporaire"""
    print("📁 Copie des fichiers vers le dossier temporaire...")
    
    # Créer le dossier temp s'il n'existe pas
    os.makedirs(TEMP_PATH, exist_ok=True)
    
    try:
        # Copier tous les fichiers CSV et JSON
        for filename in os.listdir(DATA_INPUT_PATH):
            if filename.endswith(('.csv', '.json')):
                src_path = os.path.join(DATA_INPUT_PATH, filename)
                dst_path = os.path.join(TEMP_PATH, filename)
                shutil.copy2(src_path, dst_path)
                print(f"   📄 Copié: {filename}")
        
        print(f"✅ Fichiers copiés vers {TEMP_PATH}")
        
    except Exception as e:
        print(f"❌ Erreur lors de la copie des fichiers: {e}")
        raise

def cleanup_temp_directory():
    """Supprime tous les fichiers du dossier temporaire"""
    print("🧹 Nettoyage du dossier temporaire...")
    
    try:
        if os.path.exists(TEMP_PATH):
            for filename in os.listdir(TEMP_PATH):
                file_path = os.path.join(TEMP_PATH, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"   🗑️  Supprimé: {filename}")
            
            # Supprimer le dossier temp s'il est vide
            try:
                os.rmdir(TEMP_PATH)
                print("   🗂️  Dossier temporaire supprimé")
            except OSError:
                print("   ℹ️  Dossier temporaire non vide, conservé")
        
        print("✅ Nettoyage terminé")
        
    except Exception as e:
        print(f"❌ Erreur lors du nettoyage: {e}")
        # Ne pas faire échouer le script pour une erreur de nettoyage

def generate_daily_summary_csv(session, target_date=None):
    """Génère le fichier CSV de résumé quotidien depuis PostgreSQL"""
    from datetime import date
    
    if target_date is None:
        target_date = TARGET_DATE
    
    print(f"📊 Génération du CSV de résumé pour le {target_date}...")
    
    try:
        # Créer le dossier de sortie s'il n'existe pas
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        
        # Requête SQL pour générer le résumé quotidien par ville et canal
        # Similaire à ce qui est fait dans Partie_2 mais avec les données PostgreSQL
        query = """
        WITH order_totals AS (
            SELECT 
                o.order_id,
                o.customer_id,
                o.channel,
                DATE(o.created_at) as order_date,
                c.city,
                SUM(oi.qty * oi.unit_price) as gross_revenue,
                COALESCE(SUM(r.amount), 0) as refunds_amount
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN refunds r ON o.order_id = r.order_id
            WHERE DATE(o.created_at) = :target_date
            GROUP BY o.order_id, o.customer_id, o.channel, DATE(o.created_at), c.city
        )
        SELECT 
            order_date as date,
            city,
            channel,
            COUNT(*) as orders_count,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(gross_revenue) as gross_revenue_eur,
            SUM(refunds_amount) as refunds_eur,
            SUM(gross_revenue - refunds_amount) as net_revenue_eur
        FROM order_totals
        GROUP BY order_date, city, channel
        ORDER BY city, channel
        """
        
        # Exécuter la requête
        result = session.execute(text(query), {"target_date": target_date})
        
        # Convertir en DataFrame
        df = pd.DataFrame(result.fetchall(), columns=[
            'date', 'city', 'channel', 'orders_count', 'unique_customers', 
            'gross_revenue_eur', 'refunds_eur', 'net_revenue_eur'
        ])
        
        # Ajouter le calcul des items vendus (nécessite une requête supplémentaire)
        items_query = """
        SELECT 
            c.city,
            o.channel,
            SUM(oi.qty) as items_sold
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE DATE(o.created_at) = :target_date
        GROUP BY c.city, o.channel
        ORDER BY c.city, o.channel
        """
        
        items_result = session.execute(text(items_query), {"target_date": target_date})
        items_df = pd.DataFrame(items_result.fetchall(), columns=['city', 'channel', 'items_sold'])
        
        # Fusionner les DataFrames
        df = df.merge(items_df, on=['city', 'channel'], how='left')
        
        # Réorganiser les colonnes dans l'ordre attendu
        df = df[['date', 'city', 'channel', 'orders_count', 'unique_customers', 
                'items_sold', 'gross_revenue_eur', 'refunds_eur', 'net_revenue_eur']]
        
        # Générer le nom du fichier
        filename = f"daily_summary_{target_date.strftime('%Y%m%d')}.csv"
        filepath = os.path.join(OUTPUT_PATH, filename)
        
        # Sauvegarder en CSV avec le bon format (point-virgule)
        df.to_csv(filepath, index=False, sep=';', encoding='utf-8')
        
        print(f"✅ CSV généré: {filename}")
        print(f"   📊 {len(df)} lignes exportées")
        print(f"   📁 Sauvegardé dans: {filepath}")
        
        return filepath
        
    except Exception as e:
        print(f"❌ Erreur lors de la génération du CSV: {e}")
        raise

def populate_orders_clean(session, target_date=None):
    """Peuple la table orders_clean avec les données agrégées"""
    from datetime import date
    
    if target_date is None:
        target_date = TARGET_DATE
    
    print(f"📊 Peuplement de la table orders_clean pour le {target_date}...")
    
    try:
        # Nettoyer les données existantes pour cette date
        session.query(OrderClean).filter(
            OrderClean.order_date.cast(String).like(f'{target_date}%')
        ).delete(synchronize_session=False)
        
        # Requête pour générer les données orders_clean
        query = """
        WITH order_aggregates AS (
            SELECT 
                o.order_id,
                o.customer_id,
                o.channel,
                o.created_at as order_date,
                c.city,
                SUM(oi.qty) as items_sold,
                SUM(oi.qty * oi.unit_price) as gross_revenue,
                COALESCE(SUM(r.amount), 0) as refunds_amount
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN refunds r ON o.order_id = r.order_id
            WHERE DATE(o.created_at) = :target_date
            GROUP BY o.order_id, o.customer_id, o.channel, o.created_at, c.city
        )
        SELECT 
            order_id,
            customer_id,
            channel,
            city,
            order_date,
            items_sold,
            gross_revenue as gross_revenue_eur,
            refunds_amount as refunds_eur,
            (gross_revenue + refunds_amount) as net_revenue_eur
        FROM order_aggregates
        ORDER BY order_date, order_id
        """
        
        result = session.execute(text(query), {"target_date": target_date})
        
        orders_count = 0
        for row in result.fetchall():
            order_clean = OrderClean(
                order_id=row[0],
                customer_id=row[1],
                channel=row[2],
                city=row[3],
                order_date=row[4],
                items_sold=row[5],
                gross_revenue_eur=row[6],
                refunds_eur=row[7],
                net_revenue_eur=row[8]
            )
            session.add(order_clean)
            orders_count += 1
        
        session.commit()
        print(f"✅ {orders_count} commandes ajoutées à orders_clean")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Erreur lors du peuplement de orders_clean: {e}")
        raise

def populate_daily_city_sales(session, target_date=None):
    """Peuple la table daily_city_sales avec les résumés quotidiens"""
    from datetime import date
    
    if target_date is None:
        target_date = TARGET_DATE
    
    print(f"📊 Peuplement de la table daily_city_sales pour le {target_date}...")
    
    try:
        # Nettoyer les données existantes pour cette date
        session.query(DailyCitySales).filter(
            DailyCitySales.date.cast(String).like(f'{target_date}%')
        ).delete(synchronize_session=False)
        
        # Requête pour générer les données daily_city_sales
        query = """
        WITH order_totals AS (
            SELECT 
                o.order_id,
                o.customer_id,
                o.channel,
                DATE(o.created_at) as order_date,
                c.city,
                SUM(oi.qty * oi.unit_price) as gross_revenue,
                COALESCE(SUM(r.amount), 0) as refunds_amount
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN refunds r ON o.order_id = r.order_id
            WHERE DATE(o.created_at) = :target_date
            GROUP BY o.order_id, o.customer_id, o.channel, DATE(o.created_at), c.city
        ),
        daily_summary AS (
            SELECT 
                order_date as date,
                city,
                channel,
                COUNT(*) as orders_count,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(gross_revenue) as gross_revenue_eur,
                SUM(refunds_amount) as refunds_eur,
                SUM(gross_revenue + refunds_amount) as net_revenue_eur
            FROM order_totals
            GROUP BY order_date, city, channel
        ),
        items_summary AS (
            SELECT 
                c.city,
                o.channel,
                SUM(oi.qty) as items_sold
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE DATE(o.created_at) = :target_date
            GROUP BY c.city, o.channel
        )
        SELECT 
            ds.date,
            ds.city,
            ds.channel,
            ds.orders_count,
            ds.unique_customers,
            COALESCE(is_sum.items_sold, 0) as items_sold,
            ds.gross_revenue_eur,
            ds.refunds_eur,
            ds.net_revenue_eur
        FROM daily_summary ds
        LEFT JOIN items_summary is_sum ON ds.city = is_sum.city AND ds.channel = is_sum.channel
        ORDER BY ds.city, ds.channel
        """
        
        result = session.execute(text(query), {"target_date": target_date})
        
        summaries_count = 0
        for row in result.fetchall():
            daily_summary = DailyCitySales(
                date=row[0],
                city=row[1],
                channel=row[2],
                orders_count=row[3],
                unique_customers=row[4],
                items_sold=row[5],
                gross_revenue_eur=row[6],
                refunds_eur=row[7],
                net_revenue_eur=row[8]
            )
            session.add(daily_summary)
            summaries_count += 1
        
        session.commit()
        print(f"✅ {summaries_count} résumés quotidiens ajoutés à daily_city_sales")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Erreur lors du peuplement de daily_city_sales: {e}")
        raise

def extract_date_from_filename(filename):
    """Extrait la date depuis le nom de fichier orders_YYYY-MM-DD.json"""
    match = re.search(r'orders_(\d{4}-\d{2}-\d{2})\.json', filename)
    if match:
        return match.group(1)
    return None

def cleanup_date_data(session, date_str):
    """Supprime toutes les données d'une date spécifique"""
    print(f"🗑️  Suppression des données du {date_str}...")
    
    try:
        # 1. Trouver les order_ids des commandes de cette date
        orders_of_date = session.query(Order.order_id).filter(
            Order.created_at.cast(DateTime).cast(String).like(f'{date_str}%')
        ).all()
        
        if not orders_of_date:
            print(f"   ℹ️  Aucune donnée à supprimer pour le {date_str}")
            return
        
        order_ids = [order[0] for order in orders_of_date]
        
        # 2. Supprimer les refunds de ces commandes
        refunds_deleted = session.query(Refund).filter(
            Refund.order_id.in_(order_ids)
        ).delete(synchronize_session=False)
        
        # 3. Supprimer les order_items de ces commandes
        items_deleted = session.query(OrderItem).filter(
            OrderItem.order_id.in_(order_ids)
        ).delete(synchronize_session=False)
        
        # 4. Supprimer les orders de cette date
        orders_deleted = session.query(Order).filter(
            Order.created_at.cast(DateTime).cast(String).like(f'{date_str}%')
        ).delete(synchronize_session=False)
        
        session.commit()
        print(f"✅ Supprimé: {orders_deleted} commandes, {items_deleted} items, {refunds_deleted} remboursements")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Erreur lors de la suppression: {e}")
        raise

def import_customers(session):
    """Importe les données customers.csv"""
    print("📋 Import des clients...")
    
    try:
        # Lire le fichier CSV depuis le dossier temporaire
        df = pd.read_csv(os.path.join(TEMP_PATH, 'customers.csv'))
        print(f"   📊 {len(df)} clients trouvés")
        
        # Insérer tous les clients (sans filtrage)
        for _, row in df.iterrows():
            customer = Customer(
                customer_id=row['customer_id'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                email=row['email'],
                city=row['city'],
                is_active=row['is_active']
            )
            session.add(customer)
        
        session.commit()
        print(f"✅ {len(df)} clients importés")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Erreur lors de l'import des clients: {e}")
        raise

def import_orders_for_date(session, file_path):
    """Importe les commandes d'un fichier JSON spécifique"""
    filename = os.path.basename(file_path)
    date_str = extract_date_from_filename(filename)
    
    if not date_str:
        print(f"⚠️  Impossible d'extraire la date du fichier {filename}")
        return
    
    print(f"📦 Import des commandes du {date_str}...")
    
    # Nettoyer les données existantes pour cette date
    cleanup_date_data(session, date_str)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        
        orders_count = 0
        items_count = 0
        seen_orders = set()  # Pour dédupliquer sur order_id
        
        for order_data in orders_data:
            order_id = order_data['order_id']
            
            # Dédupliquer sur order_id (garder première occurrence)
            if order_id in seen_orders:
                continue
            seen_orders.add(order_id)
            
            # Créer l'objet Order
            order = Order(
                order_id=order_id,
                customer_id=order_data['customer_id'],
                channel=order_data['channel'],
                created_at=datetime.strptime(order_data['created_at'], '%Y-%m-%d %H:%M:%S'),
                payment_status=order_data['payment_status']
            )
            session.add(order)
            orders_count += 1
            
            # Ajouter les items de la commande
            for item_data in order_data['items']:
                order_item = OrderItem(
                    order_id=order_id,
                    sku=item_data['sku'],
                    qty=item_data['qty'],
                    unit_price=float(item_data['unit_price'])  # Conversion en float pour les décimaux
                )
                session.add(order_item)
                items_count += 1
        
        session.commit()
        print(f"✅ {orders_count} commandes et {items_count} items importés pour le {date_str}")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Erreur lors de l'import des commandes du {date_str}: {e}")
        raise

def import_refunds(session):
    """Importe les remboursements depuis refunds.csv"""
    print("💰 Import des remboursements...")
    
    try:
        # Lire le fichier CSV depuis le dossier temporaire
        df = pd.read_csv(os.path.join(TEMP_PATH, 'refunds.csv'))
        print(f"   📊 {len(df)} remboursements trouvés")
        
        # Vérifier quelles commandes existent
        existing_orders = set()
        for order in session.query(Order.order_id).all():
            existing_orders.add(order[0])
        
        refunds_imported = 0
        refunds_orphaned = 0
        
        # Insérer seulement les remboursements dont les commandes existent
        for _, row in df.iterrows():
            if row['order_id'] in existing_orders:
                refund = Refund(
                    refund_id=row['refund_id'],
                    order_id=row['order_id'],
                    amount=float(row['amount']),  # Conversion en float
                    reason=row['reason'],
                    created_at=datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S')
                )
                session.add(refund)
                refunds_imported += 1
            else:
                refunds_orphaned += 1
                print(f"   ⚠️  Remboursement orphelin ignoré: {row['refund_id']} (commande {row['order_id']} non trouvée)")
        
        session.commit()
        print(f"✅ {refunds_imported} remboursements importés")
        if refunds_orphaned > 0:
            print(f"⚠️  {refunds_orphaned} remboursements orphelins ignorés")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Erreur lors de l'import des remboursements: {e}")
        raise

def main():
    """Fonction principale d'import"""
    import sys
    
    print("🚀 Début de l'import FreshKart vers PostgreSQL")
    print(f"🔗 Connexion à: {DATABASE_URL}")
    
    try:
        # Copier les fichiers vers le dossier temporaire
        copy_files_to_temp()
        
        # Créer les tables
        create_tables()
        
        # Créer une session
        session = Session()
        
        try:
            # Vérifier si un fichier spécifique est fourni en argument
            if len(sys.argv) > 1:
                file_path = sys.argv[1]
                if os.path.exists(file_path):
                    print(f"📁 Traitement du fichier spécifique: {file_path}")
                    import_orders_for_date(session, file_path)
                else:
                    print(f"❌ Fichier non trouvé: {file_path}")
                    return 1
            else:
                # Mode par défaut : importer tous les clients et remboursements une seule fois
                # puis traiter tous les fichiers orders
                print("📋 Mode complet : import de tous les fichiers")
                
                # Import initial des clients (une seule fois)
                if session.query(Customer).count() == 0:
                    import_customers(session)
                else:
                    print("👥 Clients déjà présents, passage de l'import")
                
                # Traiter tous les fichiers orders AVANT les remboursements
                order_files = glob.glob(os.path.join(TEMP_PATH, 'orders_*.json'))
                order_files.sort()
                print(f"📁 {len(order_files)} fichiers de commandes à traiter")
                
                for file_path in order_files:
                    import_orders_for_date(session, file_path)
                
                # Import des remboursements APRÈS les commandes (une seule fois)
                if session.query(Refund).count() == 0:
                    import_refunds(session)
                else:
                    print("💰 Remboursements déjà présents, passage de l'import")
            
            print("🎉 Import terminé avec succès!")
            
            # Afficher quelques statistiques
            print("\n📈 Statistiques:")
            print(f"   👥 Clients: {session.query(Customer).count()}")
            print(f"   📦 Commandes: {session.query(Order).count()}")
            print(f"   🛒 Items: {session.query(OrderItem).count()}")
            print(f"   💰 Remboursements: {session.query(Refund).count()}")
            print(f"   🧹 Commandes nettoyées: {session.query(OrderClean).count()}")
            print(f"   📊 Résumés quotidiens: {session.query(DailyCitySales).count()}")
            
            # Générer le CSV de résumé quotidien
            generate_daily_summary_csv(session)
            
            # Peupler les tables orders_clean et daily_city_sales
            populate_orders_clean(session)
            populate_daily_city_sales(session)
            
        finally:
            session.close()
        
        # Nettoyer le dossier temporaire
        cleanup_temp_directory()
            
    except Exception as e:
        print(f"💥 Erreur fatale: {e}")
        # Nettoyer même en cas d'erreur
        cleanup_temp_directory()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
