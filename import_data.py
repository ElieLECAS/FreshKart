#!/usr/bin/env python3
"""
Script d'import des données FreshKart vers PostgreSQL
Parse les fichiers CSV et JSON et les stocke dans une base PostgreSQL
"""

import os
import json
import glob
import re
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Boolean, ForeignKey, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError

# Configuration de la base de données
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/freshkart')

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

def create_tables():
    """Crée toutes les tables si elles n'existent pas"""
    print("🏗️  Création des tables...")
    Base.metadata.create_all(engine)
    print("✅ Tables créées/vérifiées")

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
        # Lire le fichier CSV
        df = pd.read_csv('/data/input/customers.csv')
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
        # Lire le fichier CSV
        df = pd.read_csv('/data/input/refunds.csv')
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
                order_files = glob.glob('/data/input/orders_*.json')
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
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"💥 Erreur fatale: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
