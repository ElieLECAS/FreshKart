import os
import json
import glob
import re
import shutil
import sys
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct, when, lit,
    to_date, to_timestamp, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, TimestampType, BooleanType
)


class DatabaseConfig:
    """Gère la configuration de la base de données"""
    
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv(
            'DATABASE_URL',
            'postgresql://postgres:postgres@localhost:5432/freshkart'
        )
        self._config = self._parse_database_url()
        if not self._config:
            raise ValueError(f"Format DATABASE_URL invalide: {self.database_url}")
    
    def _parse_database_url(self) -> Optional[Dict[str, str]]:
        """Parse DATABASE_URL pour extraire les informations de connexion JDBC"""
        match = re.match(
            r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)',
            self.database_url
        )
        if match:
            user, password, host, port, database = match.groups()
            return {
                'user': user,
                'password': password,
                'host': host,
                'port': port,
                'database': database
            }
        return None
    
    @property
    def jdbc_url(self) -> str:
        """Retourne l'URL JDBC pour PostgreSQL"""
        return f"jdbc:postgresql://{self._config['host']}:{self._config['port']}/{self._config['database']}"
    
    @property
    def jdbc_properties(self) -> Dict[str, str]:
        """Retourne les propriétés JDBC"""
        return {
            "user": self._config['user'],
            "password": self._config['password'],
            "driver": "org.postgresql.Driver"
        }
    
    def get_psycopg2_connection_params(self) -> Dict[str, str]:
        """Retourne les paramètres de connexion pour psycopg2"""
        return {
            'host': self._config['host'],
            'port': self._config['port'],
            'database': self._config['database'],
            'user': self._config['user'],
            'password': self._config['password']
        }


class SparkManager:
    """Gère la session Spark"""
    
    def __init__(self, app_name: str = "FreshKartImport"):
        self.app_name = app_name
        self._spark: Optional[SparkSession] = None
    
    def get_spark_session(self) -> SparkSession:
        """Retourne ou crée la session Spark"""
        if self._spark is None:
            self._spark = SparkSession.builder \
                .appName(self.app_name) \
                .master("local[*]") \
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .getOrCreate()
            self._spark.sparkContext.setLogLevel("WARN")
        return self._spark
    
    def stop(self):
        """Arrête la session Spark"""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None


class FileManager:
    """Gère les opérations sur les fichiers"""
    
    def __init__(self, data_input_path: str = '/data/input',
                 temp_path: str = '/app/temp',
                 output_path: str = '/app/output'):
        self.data_input_path = data_input_path
        self.temp_path = temp_path
        self.output_path = output_path
    
    def copy_files_to_temp(self):
        """Copie les fichiers vers le dossier temporaire"""
        print("Copie des fichiers...")
        
        os.makedirs(self.temp_path, exist_ok=True)
        
        # Diagnostic : vérifier ce qui existe dans /data
        data_dir = '/data'
        if os.path.exists(data_dir):
            print(f"Le répertoire {data_dir} existe")
            try:
                contents = os.listdir(data_dir)
                print(f"Contenu de {data_dir}: {contents}")
            except Exception as e:
                print(f"Impossible de lister le contenu de {data_dir}: {e}")
        else:
            print(f"ATTENTION: Le répertoire {data_dir} n'existe pas")
        
        # Vérifier que le répertoire source existe
        if not os.path.exists(self.data_input_path):
            error_msg = (
                f"Le répertoire {self.data_input_path} n'existe pas.\n"
                f"Vérifiez que le volume est correctement monté dans docker-compose.yml.\n"
                f"Le volume devrait monter le dossier 'data' vers '/data' dans le conteneur."
            )
            raise FileNotFoundError(error_msg)
        
        try:
            files_found = False
            for filename in os.listdir(self.data_input_path):
                if filename.endswith(('.csv', '.json')):
                    src_path = os.path.join(self.data_input_path, filename)
                    dst_path = os.path.join(self.temp_path, filename)
                    shutil.copy2(src_path, dst_path)
                    print(f"Copié: {filename}")
                    files_found = True
            
            if not files_found:
                print(f"Aucun fichier CSV ou JSON trouvé dans {self.data_input_path}")
            else:
                print(f"Fichiers copiés vers {self.temp_path}")
        
        except Exception as e:
            print(f"Erreur lors de la copie des fichiers: {e}")
            raise
    
    def cleanup_temp_directory(self):
        """Supprime complètement le contenu du dossier temporaire (sans supprimer le dossier lui-même)"""
        print("Nettoyage du dossier temporaire...")
        
        try:
            if not os.path.exists(self.temp_path):
                print("Dossier temporaire n'existe pas")
                print("Nettoyage terminé")
                return
            
            # Supprimer uniquement le contenu du dossier, pas le dossier lui-même
            # Cela évite l'erreur "Device or resource busy" si le dossier est un point de montage
            if not os.path.isdir(self.temp_path):
                # Si c'est un fichier et non un dossier, le supprimer
                try:
                    os.remove(self.temp_path)
                    print("Fichier temporaire supprimé")
                except OSError as e:
                    print(f"Impossible de supprimer le fichier temporaire: {e}")
                print("Nettoyage terminé")
                return
            
            # Lister et supprimer le contenu du dossier
            try:
                items = os.listdir(self.temp_path)
            except OSError as e:
                print(f"Impossible de lister le contenu du dossier temporaire: {e}")
                print("Nettoyage terminé")
                return
            
            deleted_count = 0
            failed_items = []
            
            for item in items:
                item_path = os.path.join(self.temp_path, item)
                try:
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.remove(item_path)
                        deleted_count += 1
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                        deleted_count += 1
                except OSError as e:
                    # Enregistrer les éléments qui n'ont pas pu être supprimés
                    failed_items.append((item, str(e)))
                    continue
            
            if deleted_count > 0:
                print(f"{deleted_count} élément(s) supprimé(s) du dossier temporaire")
            
            if failed_items:
                print(f"Avertissement: {len(failed_items)} élément(s) n'ont pas pu être supprimés:")
                for item, error in failed_items[:5]:  # Limiter à 5 pour ne pas surcharger la sortie
                    print(f"  - {item}: {error}")
                if len(failed_items) > 5:
                    print(f"  ... et {len(failed_items) - 5} autre(s)")
            
            # Vérifier si le dossier est maintenant vide
            try:
                remaining = os.listdir(self.temp_path)
                if not remaining:
                    print("Dossier temporaire vidé avec succès")
                else:
                    print(f"Avertissement: {len(remaining)} élément(s) restant(s) dans le dossier temporaire")
            except OSError:
                # Ne pas afficher d'erreur si on ne peut pas lister (peut être normal)
                pass
            
            print("Nettoyage terminé")
        
        except Exception as e:
            print(f"Erreur lors du nettoyage: {e}")
            # Ne pas lever d'exception, juste loguer l'erreur
            # Le dossier sera nettoyé lors de la prochaine exécution si nécessaire
    
    def get_order_files(self) -> List[str]:
        """Retourne la liste des fichiers de commandes"""
        order_files = glob.glob(os.path.join(self.temp_path, 'orders_*.json'))
        order_files.sort()
        return order_files
    
    @staticmethod
    def extract_date_from_filename(filename: str) -> Optional[str]:
        """Extrait la date du nom de fichier"""
        match = re.search(r'orders_(\d{4}-\d{2}-\d{2})\.json', filename)
        if match:
            return match.group(1)
        return None


class DatabaseManager:
    """Gère les opérations sur la base de données"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
    
    def create_tables(self):
        """Crée toutes les tables en utilisant Spark SQL via JDBC"""
        print("Création des tables...")
        
        try:
            import psycopg2
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
            
            conn = psycopg2.connect(**self.db_config.get_psycopg2_connection_params())
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Créer les tables si elles n'existent pas
            tables_sql = [
                """
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id VARCHAR(10) PRIMARY KEY,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    city VARCHAR(100) NOT NULL,
                    is_active BOOLEAN NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id VARCHAR(20) PRIMARY KEY,
                    customer_id VARCHAR(10) NOT NULL,
                    channel VARCHAR(20) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    payment_status VARCHAR(20) NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS order_items (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(20) NOT NULL,
                    sku VARCHAR(50) NOT NULL,
                    qty INTEGER NOT NULL,
                    unit_price FLOAT NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS refunds (
                    refund_id VARCHAR(20) PRIMARY KEY,
                    order_id VARCHAR(20) NOT NULL,
                    amount FLOAT NOT NULL,
                    reason VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(order_id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS orders_clean (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(20) NOT NULL,
                    customer_id VARCHAR(10) NOT NULL,
                    channel VARCHAR(20) NOT NULL,
                    city VARCHAR(100) NOT NULL,
                    order_date TIMESTAMP NOT NULL,
                    items_sold INTEGER NOT NULL,
                    gross_revenue_eur FLOAT NOT NULL,
                    refunds_eur FLOAT NOT NULL DEFAULT 0.0,
                    net_revenue_eur FLOAT NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS daily_city_sales (
                    id SERIAL PRIMARY KEY,
                    date TIMESTAMP NOT NULL,
                    city VARCHAR(100) NOT NULL,
                    channel VARCHAR(20) NOT NULL,
                    orders_count INTEGER NOT NULL,
                    unique_customers INTEGER NOT NULL,
                    items_sold INTEGER NOT NULL,
                    gross_revenue_eur FLOAT NOT NULL,
                    refunds_eur FLOAT NOT NULL,
                    net_revenue_eur FLOAT NOT NULL
                )
                """
            ]
            
            for sql in tables_sql:
                cursor.execute(sql)
            
            cursor.close()
            conn.close()
            print("Tables créées")
        
        except Exception as e:
            print(f"Erreur lors de la création des tables: {e}")
            raise
    
    def cleanup_date_data(self, date_str: str):
        """Supprime les données d'une date en utilisant JDBC"""
        print(f"Suppression des données du {date_str}...")
        
        try:
            import psycopg2
            
            conn = psycopg2.connect(**self.db_config.get_psycopg2_connection_params())
            cursor = conn.cursor()
            
            # Trouver les order_ids pour cette date
            cursor.execute(
                "SELECT order_id FROM orders WHERE DATE(created_at) = %s",
                (date_str,)
            )
            order_ids = [row[0] for row in cursor.fetchall()]
            
            if not order_ids:
                print(f"Aucune donnée à supprimer pour le {date_str}")
                cursor.close()
                conn.close()
                return
            
            # Supprimer dans l'ordre (refunds, order_items, puis orders)
            if order_ids:
                placeholders = ','.join(['%s'] * len(order_ids))
                cursor.execute(f"DELETE FROM refunds WHERE order_id IN ({placeholders})", order_ids)
                refunds_deleted = cursor.rowcount
                
                cursor.execute(f"DELETE FROM order_items WHERE order_id IN ({placeholders})", order_ids)
                items_deleted = cursor.rowcount
                
                cursor.execute(f"DELETE FROM orders WHERE order_id IN ({placeholders})", order_ids)
                orders_deleted = cursor.rowcount
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"Supprimé: {orders_deleted} commandes, {items_deleted} items, {refunds_deleted} remboursements")
        
        except Exception as e:
            print(f"Erreur lors de la suppression: {e}")
            raise
    
    def delete_orders_clean_for_date(self, date_str: str):
        """Supprime les données de orders_clean pour une date"""
        import psycopg2
        conn = psycopg2.connect(**self.db_config.get_psycopg2_connection_params())
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM orders_clean WHERE DATE(order_date) = %s",
            (date_str,)
        )
        conn.commit()
        cursor.close()
        conn.close()
    
    def delete_daily_city_sales_for_date(self, date_str: str):
        """Supprime les données de daily_city_sales pour une date"""
        import psycopg2
        conn = psycopg2.connect(**self.db_config.get_psycopg2_connection_params())
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM daily_city_sales WHERE DATE(date) = %s",
            (date_str,)
        )
        conn.commit()
        cursor.close()
        conn.close()


class DataImporter:
    """Gère l'import des données"""
    
    def __init__(self, spark_manager: SparkManager, db_config: DatabaseConfig, file_manager: FileManager):
        self.spark_manager = spark_manager
        self.db_config = db_config
        self.file_manager = file_manager
        self.db_manager = DatabaseManager(db_config)
    
    def import_customers(self):
        """Importe les clients en utilisant PySpark"""
        print("Import des clients...")
        
        spark = self.spark_manager.get_spark_session()
        
        try:
            # Vérifier si des clients existent déjà
            try:
                existing_customers = spark.read.jdbc(
                    self.db_config.jdbc_url,
                    "customers",
                    properties=self.db_config.jdbc_properties
                )
                if existing_customers.count() > 0:
                    print("Clients déjà présents, passage de l'import")
                    return
            except Exception:
                # La table n'existe peut-être pas encore, on continue
                pass
            
            # Lire le CSV avec Spark
            customers_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(os.path.join(self.file_manager.temp_path, 'customers.csv'))
            
            # Convertir is_active en boolean si nécessaire
            customers_df = customers_df.withColumn(
                "is_active",
                when(col("is_active").cast("string").isin(["True", "true", "1", "True ", "TRUE"]), lit(True))
                .when(col("is_active").cast("string").isin(["False", "false", "0", "False ", "FALSE"]), lit(False))
                .otherwise(col("is_active").cast(BooleanType()))
            )
            
            count_customers = customers_df.count()
            print(f"{count_customers} clients trouvés")
            
            # Écrire dans PostgreSQL
            customers_df.write \
                .mode("append") \
                .option("createTableColumnTypes", "customer_id VARCHAR(10), first_name VARCHAR(100), last_name VARCHAR(100), email VARCHAR(255), city VARCHAR(100), is_active BOOLEAN") \
                .jdbc(self.db_config.jdbc_url, "customers", properties=self.db_config.jdbc_properties)
            
            print(f"{count_customers} clients importés")
        
        except Exception as e:
            print(f"Erreur lors de l'import des clients: {e}")
            raise
    
    def import_orders_for_date(self, file_path: str):
        """Importe les commandes d'un fichier JSON en utilisant PySpark"""
        filename = os.path.basename(file_path)
        date_str = self.file_manager.extract_date_from_filename(filename)
        
        if not date_str:
            print(f"Impossible d'extraire la date du fichier {filename}")
            return
        
        print(f"Import des commandes du {date_str}...")
        
        self.db_manager.cleanup_date_data(date_str)
        
        spark = self.spark_manager.get_spark_session()
        
        try:
            # Lire le fichier JSON
            with open(file_path, 'r', encoding='utf-8') as f:
                orders_data = json.load(f)
            
            # Préparer les données pour Spark
            orders_list = []
            items_list = []
            seen_orders = set()
            
            for order_data in orders_data:
                order_id = order_data['order_id']
                
                if order_id in seen_orders:
                    continue
                seen_orders.add(order_id)
                
                # Préparer la commande
                orders_list.append({
                    'order_id': order_id,
                    'customer_id': order_data['customer_id'],
                    'channel': order_data['channel'],
                    'created_at': datetime.strptime(order_data['created_at'], '%Y-%m-%d %H:%M:%S'),
                    'payment_status': order_data['payment_status']
                })
                
                # Préparer les items
                for item_data in order_data['items']:
                    items_list.append({
                        'order_id': order_id,
                        'sku': item_data['sku'],
                        'qty': int(item_data['qty']),
                        'unit_price': float(item_data['unit_price'])
                    })
            
            if not orders_list:
                print(f"Aucune commande à importer pour le {date_str}")
                return
            
            # Créer les DataFrames Spark
            orders_df = spark.createDataFrame(orders_list)
            items_df = spark.createDataFrame(items_list)
            
            # Écrire dans PostgreSQL
            orders_df.write \
                .mode("append") \
                .jdbc(self.db_config.jdbc_url, "orders", properties=self.db_config.jdbc_properties)
            
            items_df.write \
                .mode("append") \
                .jdbc(self.db_config.jdbc_url, "order_items", properties=self.db_config.jdbc_properties)
            
            orders_count = len(orders_list)
            items_count = len(items_list)
            print(f"{orders_count} commandes et {items_count} items importés pour le {date_str}")
        
        except Exception as e:
            print(f"Erreur lors de l'import des commandes du {date_str}: {e}")
            raise
    
    def import_refunds(self):
        """Importe les remboursements en utilisant PySpark"""
        print("Import des remboursements...")
        
        spark = self.spark_manager.get_spark_session()
        
        try:
            # Vérifier si des remboursements existent déjà
            existing_refunds = spark.read.jdbc(
                self.db_config.jdbc_url,
                "refunds",
                properties=self.db_config.jdbc_properties
            )
            if existing_refunds.count() > 0:
                print("Remboursements déjà présents, passage de l'import")
                return
            
            # Lire les commandes existantes
            existing_orders_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "orders",
                properties=self.db_config.jdbc_properties
            )
            existing_order_ids = set(existing_orders_df.select("order_id").rdd.map(lambda r: r[0]).collect())
            
            # Lire le CSV avec Spark
            refunds_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(os.path.join(self.file_manager.temp_path, 'refunds.csv'))
            
            # Convertir created_at en timestamp
            refunds_df = refunds_df.withColumn(
                "created_at",
                to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss")
            )
            
            # Filtrer les remboursements orphelins
            refunds_valid = refunds_df.filter(col("order_id").isin(list(existing_order_ids)))
            refunds_orphaned = refunds_df.filter(~col("order_id").isin(list(existing_order_ids)))
            
            refunds_orphaned_count = refunds_orphaned.count()
            if refunds_orphaned_count > 0:
                print(f"{refunds_orphaned_count} remboursements orphelins ignorés")
                # Afficher quelques exemples
                orphaned_samples = refunds_orphaned.select("refund_id", "order_id").limit(5).collect()
                for sample in orphaned_samples:
                    print(f"  Remboursement orphelin ignoré: {sample['refund_id']} (commande {sample['order_id']} non trouvée)")
            
            # Écrire dans PostgreSQL
            refunds_valid.write \
                .mode("append") \
                .jdbc(self.db_config.jdbc_url, "refunds", properties=self.db_config.jdbc_properties)
            
            refunds_imported = refunds_valid.count()
            print(f"{refunds_imported} remboursements importés")
        
        except Exception as e:
            print(f"Erreur lors de l'import des remboursements: {e}")
            raise


class DataProcessor:
    """Gère le traitement des données"""
    
    def __init__(self, spark_manager: SparkManager, db_config: DatabaseConfig, file_manager: FileManager):
        self.spark_manager = spark_manager
        self.db_config = db_config
        self.file_manager = file_manager
        self.db_manager = DatabaseManager(db_config)
    
    def generate_daily_summary_csv(self, target_date: Optional[date] = None):
        """Génère le CSV de résumé quotidien en utilisant Spark SQL"""
        if target_date is None:
            target_date_env = os.getenv('TARGET_DATE')
            if target_date_env:
                target_date = date.fromisoformat(target_date_env)
            else:
                target_date = date.today() - timedelta(days=1)
        
        print(f"Génération du CSV pour le {target_date}...")
        
        spark = self.spark_manager.get_spark_session()
        
        try:
            os.makedirs(self.file_manager.output_path, exist_ok=True)
            
            # Lire les tables depuis PostgreSQL
            orders_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "orders",
                properties=self.db_config.jdbc_properties
            )
            customers_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "customers",
                properties=self.db_config.jdbc_properties
            )
            order_items_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "order_items",
                properties=self.db_config.jdbc_properties
            )
            refunds_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "refunds",
                properties=self.db_config.jdbc_properties
            )
            
            # Convertir created_at en date pour le filtrage
            orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
            
            # Filtrer par date cible
            target_date_str = target_date.strftime('%Y-%m-%d')
            orders_filtered = orders_df.filter(col("order_date") == target_date_str)
            
            # Calculer les revenus bruts par commande
            order_revenues = order_items_df.groupBy("order_id").agg(
                spark_sum(col("qty") * col("unit_price")).alias("gross_revenue")
            )
            
            # Calculer les remboursements par commande
            order_refunds = refunds_df.groupBy("order_id").agg(
                spark_sum("amount").alias("refunds_amount")
            )
            
            # Joindre toutes les données
            order_totals = orders_filtered \
                .join(customers_df, "customer_id", "inner") \
                .join(order_revenues, "order_id", "left") \
                .join(order_refunds, "order_id", "left") \
                .withColumn("gross_revenue", coalesce(col("gross_revenue"), lit(0.0))) \
                .withColumn("refunds_amount", coalesce(col("refunds_amount"), lit(0.0)))
            
            # Calculer les items vendus par ville et canal
            items_by_city_channel = orders_filtered \
                .join(customers_df, "customer_id", "inner") \
                .join(order_items_df, "order_id", "inner") \
                .groupBy("city", "channel") \
                .agg(spark_sum("qty").alias("items_sold"))
            
            # Agréger par ville et canal
            daily_summary = order_totals \
                .groupBy("order_date", "city", "channel") \
                .agg(
                    count("order_id").alias("orders_count"),
                    countDistinct("customer_id").alias("unique_customers"),
                    spark_sum("gross_revenue").alias("gross_revenue_eur"),
                    spark_sum("refunds_amount").alias("refunds_eur")
                ) \
                .withColumn("net_revenue_eur", col("gross_revenue_eur") - col("refunds_eur")) \
                .join(items_by_city_channel, ["city", "channel"], "left") \
                .withColumn("items_sold", coalesce(col("items_sold"), lit(0))) \
                .select(
                    col("order_date").alias("date"),
                    "city",
                    "channel",
                    "orders_count",
                    "unique_customers",
                    "items_sold",
                    "gross_revenue_eur",
                    "refunds_eur",
                    "net_revenue_eur"
                ) \
                .orderBy("city", "channel")
            
            filename = f"daily_summary_{target_date.strftime('%Y%m%d')}.csv"
            temp_dir = os.path.join(self.file_manager.output_path, f"temp_{filename}")
            final_path = os.path.join(self.file_manager.output_path, filename)
            
            # Écrire le CSV avec séparateur point-virgule dans un dossier temporaire
            daily_summary.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .option("sep", ";") \
                .csv(temp_dir)
            
            # Renommer le fichier de sortie
            csv_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
            if csv_files:
                # Supprimer le fichier ou dossier final s'il existe déjà
                if os.path.exists(final_path):
                    if os.path.isdir(final_path):
                        shutil.rmtree(final_path)
                    else:
                        os.remove(final_path)
                # Déplacer le fichier part-* vers le fichier final
                shutil.move(csv_files[0], final_path)
                # Supprimer le dossier temporaire
                try:
                    shutil.rmtree(temp_dir)
                except Exception:
                    pass
            else:
                raise Exception(f"Aucun fichier CSV généré dans {temp_dir}")
            
            count_rows = daily_summary.count()
            print(f"CSV généré: {filename}")
            print(f"{count_rows} lignes exportées")
            
            return final_path
        
        except Exception as e:
            print(f"Erreur lors de la génération du CSV: {e}")
            raise
    
    def populate_orders_clean(self, target_date: Optional[date] = None):
        """Peuple la table orders_clean en utilisant Spark SQL"""
        if target_date is None:
            target_date_env = os.getenv('TARGET_DATE')
            if target_date_env:
                target_date = date.fromisoformat(target_date_env)
            else:
                target_date = date.today() - timedelta(days=1)
        
        print(f"Peuplement de orders_clean pour le {target_date}...")
        
        spark = self.spark_manager.get_spark_session()
        
        try:
            target_date_str = target_date.strftime('%Y-%m-%d')
            
            # Supprimer les données existantes pour cette date
            self.db_manager.delete_orders_clean_for_date(target_date_str)
            
            # Lire les données depuis PostgreSQL
            orders_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "orders",
                properties=self.db_config.jdbc_properties
            )
            customers_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "customers",
                properties=self.db_config.jdbc_properties
            )
            order_items_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "order_items",
                properties=self.db_config.jdbc_properties
            )
            refunds_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "refunds",
                properties=self.db_config.jdbc_properties
            )
            
            # Filtrer par date
            orders_df = orders_df.withColumn("order_date_col", to_date(col("created_at")))
            orders_filtered = orders_df.filter(col("order_date_col") == target_date_str)
            
            # Agréger les items par commande
            items_agg = order_items_df.groupBy("order_id").agg(
                spark_sum("qty").alias("items_sold"),
                spark_sum(col("qty") * col("unit_price")).alias("gross_revenue")
            )
            
            # Agréger les remboursements par commande
            refunds_agg = refunds_df.groupBy("order_id").agg(
                spark_sum("amount").alias("refunds_amount")
            )
            
            # Joindre toutes les données
            orders_clean_df = orders_filtered \
                .join(customers_df, "customer_id", "inner") \
                .join(items_agg, "order_id", "left") \
                .join(refunds_agg, "order_id", "left") \
                .withColumn("items_sold", coalesce(col("items_sold"), lit(0))) \
                .withColumn("gross_revenue_eur", coalesce(col("gross_revenue"), lit(0.0))) \
                .withColumn("refunds_eur", coalesce(col("refunds_amount"), lit(0.0))) \
                .withColumn("net_revenue_eur", col("gross_revenue_eur") - col("refunds_eur")) \
                .select(
                    "order_id",
                    "customer_id",
                    "channel",
                    "city",
                    col("created_at").alias("order_date"),
                    "items_sold",
                    "gross_revenue_eur",
                    "refunds_eur",
                    "net_revenue_eur"
                ) \
                .orderBy("order_date", "order_id")
            
            # Écrire dans PostgreSQL
            orders_clean_df.write \
                .mode("append") \
                .jdbc(self.db_config.jdbc_url, "orders_clean", properties=self.db_config.jdbc_properties)
            
            orders_count = orders_clean_df.count()
            print(f"{orders_count} commandes ajoutées à orders_clean")
        
        except Exception as e:
            print(f"Erreur lors du peuplement de orders_clean: {e}")
            raise
    
    def populate_daily_city_sales(self, target_date: Optional[date] = None):
        """Peuple la table daily_city_sales en utilisant Spark SQL"""
        if target_date is None:
            target_date_env = os.getenv('TARGET_DATE')
            if target_date_env:
                target_date = date.fromisoformat(target_date_env)
            else:
                target_date = date.today() - timedelta(days=1)
        
        print(f"Peuplement de daily_city_sales pour le {target_date}...")
        
        spark = self.spark_manager.get_spark_session()
        
        try:
            target_date_str = target_date.strftime('%Y-%m-%d')
            
            # Supprimer les données existantes pour cette date
            self.db_manager.delete_daily_city_sales_for_date(target_date_str)
            
            # Lire les données depuis PostgreSQL
            orders_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "orders",
                properties=self.db_config.jdbc_properties
            )
            customers_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "customers",
                properties=self.db_config.jdbc_properties
            )
            order_items_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "order_items",
                properties=self.db_config.jdbc_properties
            )
            refunds_df = spark.read.jdbc(
                self.db_config.jdbc_url,
                "refunds",
                properties=self.db_config.jdbc_properties
            )
            
            # Filtrer par date
            orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
            orders_filtered = orders_df.filter(col("order_date") == target_date_str)
            
            # Calculer les revenus et remboursements par commande
            order_revenues = order_items_df.groupBy("order_id").agg(
                spark_sum(col("qty") * col("unit_price")).alias("gross_revenue")
            )
            
            order_refunds = refunds_df.groupBy("order_id").agg(
                spark_sum("amount").alias("refunds_amount")
            )
            
            # Joindre les données
            order_totals = orders_filtered \
                .join(customers_df, "customer_id", "inner") \
                .join(order_revenues, "order_id", "left") \
                .join(order_refunds, "order_id", "left") \
                .withColumn("gross_revenue", coalesce(col("gross_revenue"), lit(0.0))) \
                .withColumn("refunds_amount", coalesce(col("refunds_amount"), lit(0.0)))
            
            # Calculer les items vendus
            items_by_city_channel = orders_filtered \
                .join(customers_df, "customer_id", "inner") \
                .join(order_items_df, "order_id", "inner") \
                .groupBy("city", "channel") \
                .agg(spark_sum("qty").alias("items_sold"))
            
            # Agréger par ville et canal
            daily_summary_df = order_totals \
                .groupBy("order_date", "city", "channel") \
                .agg(
                    count("order_id").alias("orders_count"),
                    countDistinct("customer_id").alias("unique_customers"),
                    spark_sum("gross_revenue").alias("gross_revenue_eur"),
                    spark_sum("refunds_amount").alias("refunds_eur")
                ) \
                .withColumn("net_revenue_eur", col("gross_revenue_eur") - col("refunds_eur")) \
                .join(items_by_city_channel, ["city", "channel"], "left") \
                .withColumn("items_sold", coalesce(col("items_sold"), lit(0))) \
                .select(
                    col("order_date").alias("date"),
                    "city",
                    "channel",
                    "orders_count",
                    "unique_customers",
                    "items_sold",
                    "gross_revenue_eur",
                    "refunds_eur",
                    "net_revenue_eur"
                ) \
                .orderBy("city", "channel")
            
            # Écrire dans PostgreSQL
            daily_summary_df.write \
                .mode("append") \
                .jdbc(self.db_config.jdbc_url, "daily_city_sales", properties=self.db_config.jdbc_properties)
            
            summaries_count = daily_summary_df.count()
            print(f"{summaries_count} résumés ajoutés à daily_city_sales")
        
        except Exception as e:
            print(f"Erreur lors du peuplement de daily_city_sales: {e}")
            raise


class FreshKartImport:
    """Classe principale pour orchestrer l'import FreshKart"""
    
    def __init__(self, database_url: Optional[str] = None):
        self.db_config = DatabaseConfig(database_url)
        self.spark_manager = SparkManager()
        self.file_manager = FileManager()
        self.data_importer = DataImporter(
            self.spark_manager,
            self.db_config,
            self.file_manager
        )
        self.data_processor = DataProcessor(
            self.spark_manager,
            self.db_config,
            self.file_manager
        )
        self.db_manager = DatabaseManager(self.db_config)
    
    def run(self, file_path: Optional[str] = None) -> int:
        """Exécute le processus d'import complet"""
        print("Début de l'import FreshKart")
        print(f"Connexion à: {self.db_config.database_url}")
        
        try:
            self.file_manager.copy_files_to_temp()
            self.db_manager.create_tables()
            
            if file_path and os.path.exists(file_path):
                print(f"Traitement du fichier: {file_path}")
                self.data_importer.import_orders_for_date(file_path)
            elif file_path:
                print(f"Fichier non trouvé: {file_path}")
                return 1
            else:
                print("Mode complet : import de tous les fichiers")
                
                self.data_importer.import_customers()
                
                order_files = self.file_manager.get_order_files()
                print(f"{len(order_files)} fichiers de commandes à traiter")
                
                for file_path in order_files:
                    self.data_importer.import_orders_for_date(file_path)
                
                self.data_importer.import_refunds()
            
            print("Import terminé avec succès")
            
            self.data_processor.generate_daily_summary_csv()
            self.data_processor.populate_orders_clean()
            self.data_processor.populate_daily_city_sales()
            
            return 0
        
        except Exception as e:
            print(f"Erreur fatale: {e}")
            import traceback
            traceback.print_exc()
            return 1
        
        finally:
            # Toujours arrêter Spark et nettoyer le dossier temporaire à la fin
            self.spark_manager.stop()
            self.file_manager.cleanup_temp_directory()


def main():
    """Fonction principale"""
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    importer = FreshKartImport()
    return importer.run(file_path)


if __name__ == "__main__":
    exit(main())
