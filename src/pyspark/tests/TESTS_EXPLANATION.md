# Documentation des Tests - Import Data

Ce document explique en détail chaque test unitaire présent dans le fichier `test_import_data.py`.

## Vue d'ensemble

Les tests couvrent les différentes classes du module `import_data` :
- `DatabaseConfig` : Configuration de la base de données
- `SparkManager` : Gestionnaire de session Spark
- `FileManager` : Gestionnaire de fichiers
- `DatabaseManager` : Gestionnaire de base de données
- `DataImporter` : Importateur de données
- `FreshKartImport` : Classe principale d'import

---

## TestDatabaseConfig

Cette classe teste la configuration de la base de données PostgreSQL.

### `test_init_from_env`

**Objectif** : Vérifier que la classe `DatabaseConfig` peut s'initialiser correctement à partir des variables d'environnement.

**Ce qui est testé** :
- L'extraction des paramètres de connexion depuis la variable d'environnement `DATABASE_URL`
- Le format attendu est : `postgresql://user:password@host:port/database`

**Comportement attendu** :
- Les paramètres extraits (user, password, host, port, database) doivent correspondre aux valeurs dans l'URL
- Exemple : `postgresql://env_user:env_pass@env_host:5433/env_db` doit produire :
  - user = 'env_user'
  - password = 'env_pass'
  - host = 'env_host'
  - port = '5433'
  - database = 'env_db'

**Technique utilisée** : Mock de la variable d'environnement avec `@patch.dict(os.environ, ...)`

---

### `test_init_invalid_url`

**Objectif** : Vérifier que la classe rejette les URLs de base de données invalides.

**Ce qui est testé** :
- La validation du format de l'URL de base de données
- La levée d'une exception `ValueError` avec le message "Format DATABASE_URL invalide" quand l'URL est mal formée

**Comportement attendu** :
- Une `ValueError` doit être levée si l'URL fournie n'est pas dans le format attendu
- Le message d'erreur doit contenir "Format DATABASE_URL invalide"

**Technique utilisée** : `pytest.raises()` pour vérifier l'exception

---

## TestSparkManager

Cette classe teste la gestion des sessions Spark.

### `test_get_spark_session_reuse`

**Objectif** : Vérifier que le gestionnaire Spark réutilise la même session Spark au lieu d'en créer une nouvelle à chaque appel.

**Ce qui est testé** :
- Le pattern Singleton pour la session Spark
- La réutilisation de la session existante lors d'appels multiples à `get_spark_session()`

**Comportement attendu** :
- Le premier appel à `get_spark_session()` crée une nouvelle session
- Les appels suivants retournent la même instance (réutilisation)
- `getOrCreate()` ne doit être appelé qu'une seule fois, même après plusieurs appels à `get_spark_session()`

**Technique utilisée** : Mock complet de `SparkSession` et de son builder pour simuler le comportement sans créer une vraie session Spark

**Avantage** : Évite la création de multiples sessions Spark, ce qui économise les ressources et améliore les performances

---

## TestFileManager

Cette classe teste la gestion des fichiers et l'extraction d'informations depuis les noms de fichiers.

### `test_extract_date_from_filename`

**Objectif** : Vérifier que la méthode peut extraire correctement une date depuis un nom de fichier au format `orders_YYYY-MM-DD.json`.

**Ce qui est testé** :
- L'extraction de la date depuis un nom de fichier structuré
- Le format attendu : `orders_2024-01-15.json` → `2024-01-15`

**Comportement attendu** :
- Pour un fichier nommé `orders_2024-01-15.json`, la méthode doit retourner `"2024-01-15"`
- La date doit être au format ISO (YYYY-MM-DD)

**Cas d'usage** : Cette fonctionnalité est importante pour identifier la date associée aux données de commandes dans les fichiers JSON.

---

### `test_extract_date_from_filename_invalid`

**Objectif** : Vérifier que la méthode gère correctement les noms de fichiers qui ne suivent pas le format attendu.

**Ce qui est testé** :
- Le comportement face à un nom de fichier invalide (sans date au format attendu)
- La gestion des cas d'erreur

**Comportement attendu** :
- Pour un fichier nommé `invalid_file.json` (sans date dans le nom), la méthode doit retourner `None`
- Aucune exception ne doit être levée

**Cas d'usage** : Permet de gérer gracieusement les fichiers qui ne suivent pas la convention de nommage attendue.

---

### `test_copy_files_to_temp_directory_not_exists`

**Objectif** : Vérifier que la méthode lève une exception appropriée quand le répertoire source n'existe pas.

**Ce qui est testé** :
- La vérification de l'existence du répertoire source
- La levée d'une exception `FileNotFoundError` quand le répertoire n'existe pas

**Comportement attendu** :
- Si le répertoire source n'existe pas, une `FileNotFoundError` doit être levée
- La méthode ne doit pas tenter de copier des fichiers depuis un répertoire inexistant

**Technique utilisée** : Mock de `os.path.exists` pour simuler un répertoire inexistant

**Cas d'usage** : Évite les erreurs silencieuses et informe clairement l'utilisateur que le répertoire source est introuvable.

---

## TestDatabaseManager

Cette classe teste les opérations de base de données.

### `test_cleanup_date_data_no_data`

**Objectif** : Vérifier que la méthode de nettoyage ne fait rien quand il n'y a pas de données à supprimer pour une date donnée.

**Ce qui est testé** :
- Le comportement de `cleanup_date_data()` quand aucune donnée n'existe pour la date spécifiée
- L'optimisation : pas de commit inutile si aucune opération n'est nécessaire

**Comportement attendu** :
- Si aucune donnée n'est trouvée pour la date `2024-01-15`, aucun commit ne doit être effectué
- La connexion à la base de données doit être établie pour vérifier l'existence des données
- Aucune transaction ne doit être ouverte si aucune donnée n'existe

**Technique utilisée** : Mock de `psycopg2.connect` et de la connexion/cursor pour simuler une base de données vide

**Avantage** : Évite les opérations inutiles sur la base de données, améliorant les performances.

---

## TestDataImporter

Cette classe teste l'importation des données depuis les fichiers vers la base de données.

### `test_import_customers_existing`

**Objectif** : Vérifier que l'import des clients évite les doublons en vérifiant d'abord si des clients existent déjà dans la base de données.

**Ce qui est testé** :
- La logique de vérification de l'existence des clients avant import
- L'optimisation : pas de lecture du fichier CSV si des clients existent déjà

**Comportement attendu** :
- Si des clients existent déjà dans la base de données (count > 0), le fichier CSV ne doit pas être lu
- La méthode `read.csv()` ne doit pas être appelée
- Cela évite les imports redondants et améliore les performances

**Technique utilisée** : Mock de Spark, du SparkManager, et de la lecture JDBC pour simuler des clients existants

**Cas d'usage** : Permet d'éviter de réimporter des données déjà présentes, ce qui est important pour les imports incrémentaux.

---

### `test_import_orders_for_date`

**Objectif** : Vérifier que l'import des commandes pour une date spécifique fonctionne correctement, incluant la transformation des données et l'écriture en base.

**Ce qui est testé** :
- La lecture d'un fichier JSON de commandes
- La transformation des données en DataFrames Spark (commandes et items)
- L'écriture dans la base de données (tables `orders` et `order_items`)
- Le nettoyage des données existantes pour la date avant l'import

**Comportement attendu** :
- Le fichier JSON doit être lu et parsé
- Deux DataFrames doivent être créés : un pour les commandes, un pour les items
- Les deux DataFrames doivent être écrits dans la base de données via JDBC
- La méthode `cleanup_date_data()` doit être appelée avant l'import pour éviter les doublons

**Structure des données testées** :
- Une commande avec `order_id`, `customer_id`, `channel`, `created_at`, `payment_status`
- Des items associés avec `sku`, `qty`, `unit_price`

**Technique utilisée** : Mock complet de Spark, de la lecture de fichiers JSON, et de l'écriture JDBC

**Cas d'usage** : C'est le cœur du processus d'import, transformant les données JSON en format relationnel pour la base de données.

---

## TestFreshKartImport

Cette classe teste la classe principale qui orchestre tout le processus d'import.

### `test_run_error`

**Objectif** : Vérifier que la méthode `run()` gère correctement les erreurs et nettoie les ressources même en cas d'échec.

**Ce qui est testé** :
- La gestion des exceptions pendant l'exécution
- Le nettoyage des ressources (session Spark, répertoire temporaire) même en cas d'erreur
- Le code de retour approprié (1 pour erreur, 0 pour succès)

**Comportement attendu** :
- Si une erreur survient (par exemple lors de la copie des fichiers), la méthode doit :
  1. Capturer l'exception
  2. Nettoyer le répertoire temporaire (`cleanup_temp_directory`)
  3. Arrêter la session Spark (`spark_manager.stop()`)
  4. Retourner le code d'erreur 1

**Technique utilisée** : Mock des méthodes pour simuler une erreur et vérifier que le nettoyage est toujours effectué

**Avantage** : Garantit que les ressources sont toujours libérées, même en cas d'erreur, évitant les fuites de mémoire et les sessions Spark orphelines.

**Pattern utilisé** : Try-finally implicite pour garantir le nettoyage

---

## Résumé des Techniques de Test Utilisées

### Mocks et Patches
- **`@patch`** : Pour remplacer des dépendances externes (Spark, os, psycopg2)
- **`Mock()`** : Pour créer des objets simulés avec des comportements personnalisés
- **`mock_open`** : Pour simuler l'ouverture de fichiers
- **`@patch.dict`** : Pour modifier temporairement les variables d'environnement

### Assertions
- **`assert`** : Vérifications de base (égalité, existence)
- **`pytest.raises()`** : Vérification des exceptions
- **`assert_called_once()`** : Vérification que les méthodes mockées sont appelées
- **`assert_not_called()`** : Vérification que les méthodes ne sont pas appelées

### Bonnes Pratiques Observées
1. **Isolation** : Chaque test est indépendant et utilise des mocks
2. **Nommage clair** : Les noms de tests décrivent ce qui est testé
3. **Documentation** : Docstrings explicatives pour chaque test
4. **Couverture** : Tests des cas normaux et des cas d'erreur
5. **Performance** : Tests vérifient les optimisations (réutilisation, évitement d'opérations inutiles)

---

## Comment Exécuter les Tests

```bash
# Exécuter tous les tests
pytest src/pyspark/tests/test_import_data.py

# Exécuter avec verbosité
pytest src/pyspark/tests/test_import_data.py -v

# Exécuter un test spécifique
pytest src/pyspark/tests/test_import_data.py::TestDatabaseConfig::test_init_from_env

# Exécuter avec couverture de code
pytest src/pyspark/tests/test_import_data.py --cov=import_data
```

