import os
import pytest
from unittest.mock import Mock, patch, mock_open

import sys
# Ajouter le répertoire parent au path pour importer import_data
test_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(test_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from import_data import (
    DatabaseConfig,
    SparkManager,
    FileManager,
    DatabaseManager,
    DataImporter,
    FreshKartImport
)


class TestDatabaseConfig:
    """Tests pour la classe DatabaseConfig"""
    
    @patch.dict(os.environ, {
        'POSTGRES_HOST': 'env_host',
        'POSTGRES_PORT': '5433',
        'POSTGRES_DB': 'env_db',
        'POSTGRES_USER': 'env_user',
        'POSTGRES_PASSWORD': 'env_pass'
    })
    def test_init_from_env(self):
        """Test l'initialisation depuis les variables d'environnement"""
        config = DatabaseConfig()
        assert config._config['user'] == 'env_user'
        assert config._config['password'] == 'env_pass'
        assert config._config['host'] == 'env_host'
        assert config._config['port'] == '5433'
        assert config._config['database'] == 'env_db'
    
    def test_init_with_defaults(self):
        """Test l'initialisation avec des valeurs par défaut (None)"""
        with patch.dict(os.environ, {}, clear=True):
            config = DatabaseConfig()
            assert config._config['user'] is None
            assert config._config['password'] is None
            assert config._config['host'] is None
            assert config._config['port'] is None
            assert config._config['database'] is None


class TestSparkManager:
    """Tests pour la classe SparkManager"""
    
    @patch('import_data.SparkSession')
    def test_get_spark_session_reuse(self, mock_spark_session):
        """Test la réutilisation d'une session Spark existante"""
        mock_builder = Mock()
        mock_spark = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        
        manager = SparkManager()
        spark1 = manager.get_spark_session()
        spark2 = manager.get_spark_session()
        
        assert spark1 == spark2
        assert mock_builder.getOrCreate.call_count == 1


class TestFileManager:
    """Tests pour la classe FileManager"""
    
    def test_extract_date_from_filename(self):
        """Test l'extraction de la date depuis le nom de fichier"""
        filename = "orders_2024-01-15.json"
        date_str = FileManager.extract_date_from_filename(filename)
        assert date_str == "2024-01-15"
    
    def test_extract_date_from_filename_invalid(self):
        """Test l'extraction avec un nom de fichier invalide"""
        filename = "invalid_file.json"
        date_str = FileManager.extract_date_from_filename(filename)
        assert date_str is None
    
    @patch('import_data.os')
    def test_copy_files_to_temp_directory_not_exists(self, mock_os):
        """Test l'erreur quand le répertoire n'existe pas"""
        mock_os.path.exists.return_value = False
        
        manager = FileManager()
        with pytest.raises(FileNotFoundError):
            manager.copy_files_to_temp()


class TestDatabaseManager:
    """Tests pour la classe DatabaseManager"""
    
    @patch('psycopg2.connect')
    def test_cleanup_date_data_no_data(self, mock_connect):
        """Test le nettoyage quand il n'y a pas de données"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        
        db_config = DatabaseConfig()
        manager = DatabaseManager(db_config)
        manager.cleanup_date_data("2024-01-15")
        
        mock_conn.commit.assert_not_called()


class TestDataImporter:
    """Tests pour la classe DataImporter"""
    
    @patch('import_data.os.path.join')
    @patch.dict('os.environ', {
        'POSTGRES_HOST': 'host',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'dbname',
        'POSTGRES_USER': 'user',
        'POSTGRES_PASSWORD': 'pass'
    })
    def test_import_customers_existing(self, mock_join):
        """Test l'import des clients quand ils existent déjà"""
        mock_spark = Mock()
        mock_spark_manager = Mock()
        mock_spark_manager.get_spark_session.return_value = mock_spark
        
        mock_existing_df = Mock()
        mock_existing_df.count.return_value = 10
        mock_read = Mock()
        mock_read.jdbc.return_value = mock_existing_df
        mock_spark.read = mock_read
        
        db_config = DatabaseConfig()
        file_manager = FileManager()
        importer = DataImporter(mock_spark_manager, db_config, file_manager)
        
        importer.import_customers()
        
        # Ne devrait pas lire le CSV
        mock_read.csv.assert_not_called()
    
    @patch('import_data.json')
    @patch('builtins.open', new_callable=mock_open)
    @patch('import_data.os.path.basename')
    def test_import_orders_for_date(self, mock_basename, mock_file, mock_json):
        """Test l'import des commandes pour une date"""
        mock_basename.return_value = 'orders_2024-01-15.json'
        mock_spark = Mock()
        mock_spark_manager = Mock()
        mock_spark_manager.get_spark_session.return_value = mock_spark
        
        orders_data = [
            {
                'order_id': 'ORD001',
                'customer_id': 'CUST001',
                'channel': 'online',
                'created_at': '2024-01-15 10:00:00',
                'payment_status': 'paid',
                'items': [
                    {'sku': 'SKU001', 'qty': 2, 'unit_price': 10.0}
                ]
            }
        ]
        mock_json.load.return_value = orders_data
        
        mock_orders_df = Mock()
        mock_items_df = Mock()
        mock_orders_write = Mock()
        mock_orders_write.mode.return_value = mock_orders_write
        mock_orders_write.jdbc = Mock()
        mock_orders_df.write = mock_orders_write
        
        mock_items_write = Mock()
        mock_items_write.mode.return_value = mock_items_write
        mock_items_write.jdbc = Mock()
        mock_items_df.write = mock_items_write
        
        mock_spark.createDataFrame.side_effect = [mock_orders_df, mock_items_df]
        
        db_config = DatabaseConfig()
        file_manager = FileManager()
        importer = DataImporter(mock_spark_manager, db_config, file_manager)
        
        with patch.object(importer.db_manager, 'cleanup_date_data'):
            importer.import_orders_for_date('/path/to/orders_2024-01-15.json')
        
        assert mock_spark.createDataFrame.call_count == 2
        mock_orders_write.jdbc.assert_called_once()
        mock_items_write.jdbc.assert_called_once()


class TestFreshKartImport:
    """Tests pour la classe FreshKartImport"""
    
    def test_run_error(self):
        """Test la gestion des erreurs"""
        importer = FreshKartImport()
        
        # Mocker les méthodes
        importer.file_manager.copy_files_to_temp = Mock(side_effect=Exception("Test error"))
        importer.file_manager.cleanup_temp_directory = Mock(return_value=None)
        importer.spark_manager.stop = Mock(return_value=None)
        
        result = importer.run()
        
        assert result == 1
        importer.spark_manager.stop.assert_called_once()
        importer.file_manager.cleanup_temp_directory.assert_called_once()
