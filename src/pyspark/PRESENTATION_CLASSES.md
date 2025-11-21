# Présentation : Classes DataImporter et DataProcessor

## Script d'import FreshKart avec Apache Spark

---

## Slide 1 : Introduction

**Texte à dire :**

Bonjour, je vais vous présenter le fonctionnement des classes `DataImporter` et `DataProcessor` de notre script d'import FreshKart. Ces deux classes utilisent Apache Spark, également appelé PySpark, pour traiter efficacement de grandes quantités de données.

Le script a été conçu pour importer des données depuis des fichiers CSV et JSON vers une base de données PostgreSQL, puis effectuer des traitements et agrégations complexes sur ces données.

---

## Slide 2 : Architecture générale

**Texte à dire :**

Avant de détailler les classes, je vais vous expliquer l'architecture générale. Notre script utilise Apache Spark pour le traitement distribué de données volumineuses.

Spark nous permet de :

-   Lire et écrire des données depuis et vers PostgreSQL via JDBC
-   Effectuer des transformations et agrégations complexes sur de grandes quantités de données
-   Optimiser automatiquement les opérations grâce à son moteur d'exécution distribué

L'avantage principal de Spark est sa capacité à traiter des millions de lignes de données de manière efficace, en distribuant le travail sur plusieurs cœurs ou machines.

---

## Slide 3 : Classe DataImporter - Vue d'ensemble

**Texte à dire :**

Commençons par la classe `DataImporter`. Cette classe est responsable de l'importation des données brutes depuis les fichiers CSV et JSON vers la base de données PostgreSQL.

Elle utilise Spark pour lire les fichiers et écrire les données dans la base. La classe contient trois méthodes principales :

-   `import_customers()` pour importer les clients
-   `import_orders_for_date()` pour importer les commandes d'une date spécifique
-   `import_refunds()` pour importer les remboursements

---

## Slide 4 : DataImporter - Import des clients

**Texte à dire :**

La première méthode, `import_customers()`, importe les données des clients depuis un fichier CSV.

Le processus fonctionne en plusieurs étapes :

1. D'abord, on vérifie si des clients existent déjà dans la base pour éviter les doublons
2. Ensuite, on lit le fichier `customers.csv` avec Spark en utilisant la méthode `spark.read.csv()`
3. On transforme la colonne `is_active` en booléen, en gérant différents formats comme "True", "true", "1", etc.
4. Enfin, on écrit les données dans PostgreSQL via JDBC avec la méthode `write.jdbc()`

L'utilisation de Spark permet une lecture distribuée du fichier CSV, ce qui est particulièrement efficace pour de gros volumes de données.

---

## Slide 5 : DataImporter - Import des commandes

**Texte à dire :**

La méthode `import_orders_for_date()` importe les commandes d'un fichier JSON pour une date spécifique.

Le processus est le suivant :

1. On extrait la date du nom de fichier, qui suit le format `orders_YYYY-MM-DD.json`
2. On supprime les données existantes pour cette date afin d'éviter les doublons
3. On lit le fichier JSON avec Python standard
4. On prépare les données en deux listes séparées : une pour les commandes et une pour les items
5. On crée des DataFrames Spark à partir de ces listes avec `spark.createDataFrame()`
6. On écrit les DataFrames dans PostgreSQL via JDBC, dans les tables `orders` et `order_items`

Points importants : on gère les doublons en vérifiant si un `order_id` a déjà été traité, et on sépare les commandes et les items dans des tables distinctes pour respecter la normalisation de la base de données.

---

## Slide 6 : DataImporter - Import des remboursements

**Texte à dire :**

La dernière méthode de `DataImporter`, `import_refunds()`, importe les remboursements depuis un fichier CSV.

Le processus comprend :

1. Une vérification si des remboursements existent déjà
2. La lecture des commandes existantes depuis PostgreSQL pour valider les références
3. La lecture du fichier `refunds.csv` avec Spark
4. La conversion de la colonne `created_at` en timestamp avec la fonction `to_timestamp()`
5. Le filtrage des remboursements orphelins, c'est-à-dire ceux qui n'ont pas de commande correspondante
6. L'écriture uniquement des remboursements valides dans PostgreSQL

Cette méthode utilise plusieurs fonctionnalités de Spark : la lecture depuis PostgreSQL avec `read.jdbc()`, la lecture de fichiers CSV, la transformation de colonnes avec `withColumn()`, et le filtrage avec `filter()`.

---

## Slide 7 : Classe DataProcessor - Vue d'ensemble

**Texte à dire :**

Passons maintenant à la classe `DataProcessor`. Cette classe effectue le traitement et l'agrégation des données importées.

Elle génère des résumés quotidiens, remplit des tables de synthèse et exporte des fichiers CSV. La classe contient trois méthodes principales :

-   `generate_daily_summary_csv()` pour générer un fichier CSV de résumé
-   `populate_orders_clean()` pour remplir une table de commandes nettoyées
-   `populate_daily_city_sales()` pour remplir une table de ventes quotidiennes par ville

Toutes ces méthodes utilisent Spark pour lire les données depuis PostgreSQL, effectuer des transformations complexes, et écrire les résultats.

---

## Slide 8 : DataProcessor - Génération du CSV quotidien

**Texte à dire :**

La méthode `generate_daily_summary_csv()` génère un fichier CSV de résumé quotidien par ville et canal de vente.

Le processus complet est le suivant :

1. On détermine la date cible, soit depuis une variable d'environnement, soit le jour précédent par défaut
2. On lit toutes les tables nécessaires depuis PostgreSQL via JDBC : `orders`, `customers`, `order_items`, et `refunds`
3. On filtre les commandes par date cible
4. On calcule les agrégations : les revenus bruts par commande, les remboursements par commande, et les items vendus par ville et canal
5. On joint les données et on agrège par ville et canal pour obtenir : le nombre de commandes, le nombre de clients uniques, les items vendus, les revenus bruts, les remboursements, et les revenus nets
6. On exporte le résultat dans un fichier CSV avec séparateur point-virgule

Cette méthode utilise intensivement les fonctionnalités d'agrégation de Spark avec `groupBy()` et `agg()`, ainsi que les jointures entre DataFrames.

---

## Slide 9 : DataProcessor - Table orders_clean

**Texte à dire :**

La méthode `populate_orders_clean()` remplit la table `orders_clean` avec des données nettoyées et agrégées par commande.

Le processus est similaire à la génération du CSV, mais avec quelques différences :

1. On supprime d'abord les données existantes pour la date cible
2. On lit les tables depuis PostgreSQL
3. On filtre les commandes par date
4. On agrège les items et remboursements par commande
5. On joint avec les données clients pour obtenir la ville
6. On calcule les métriques par commande : items vendus, revenus bruts, remboursements, et revenus nets
7. On écrit dans la table `orders_clean`

La différence principale avec la méthode précédente est que les agrégations se font au niveau commande plutôt qu'au niveau ville et canal, et on écrit dans une table PostgreSQL structurée plutôt que dans un fichier CSV.

---

## Slide 10 : DataProcessor - Table daily_city_sales

**Texte à dire :**

La méthode `populate_daily_city_sales()` remplit la table `daily_city_sales` avec des résumés quotidiens par ville et canal.

Cette méthode suit une logique très similaire à `generate_daily_summary_csv()`, mais au lieu d'exporter un fichier CSV, elle écrit les résultats directement dans une table PostgreSQL.

Le processus est identique : on supprime les données existantes pour la date, on lit, filtre, agrège et joint les données, puis on écrit dans la table `daily_city_sales`.

Cette table permet de stocker de manière persistante les résumés quotidiens pour des analyses ultérieures ou des requêtes rapides.

---

## Slide 11 : Concepts Spark - DataFrames

**Texte à dire :**

Maintenant, je vais vous expliquer les concepts Spark utilisés dans notre script.

Le concept fondamental est le DataFrame. Les DataFrames sont des structures de données tabulaires distribuées utilisées pour toutes les opérations dans notre script.

Ils permettent :

-   La lecture depuis fichiers ou bases de données
-   Les transformations et agrégations
-   L'écriture vers fichiers ou bases de données

Un DataFrame Spark est similaire à une table SQL ou à un DataFrame pandas, mais avec la capacité de distribuer les données sur plusieurs machines pour un traitement parallèle.

---

## Slide 12 : Concepts Spark - Opérations de transformation

**Texte à dire :**

Notre script utilise plusieurs opérations de transformation Spark :

-   `withColumn()` : Ajoute ou modifie une colonne dans un DataFrame
-   `filter()` : Filtre les lignes selon une condition
-   `join()` : Joint deux DataFrames, similaire à une jointure SQL
-   `groupBy().agg()` : Agrège les données selon des critères
-   `select()` : Sélectionne des colonnes spécifiques
-   `orderBy()` : Trie les données

Ces opérations sont optimisées par Spark et peuvent être exécutées en parallèle sur plusieurs partitions de données.

---

## Slide 13 : Concepts Spark - Fonctions d'agrégation

**Texte à dire :**

Pour les agrégations, nous utilisons plusieurs fonctions Spark :

-   `count()` : Compte le nombre de lignes
-   `countDistinct()` : Compte les valeurs distinctes d'une colonne
-   `spark_sum()` : Calcule la somme des valeurs
-   `coalesce()` : Gère les valeurs nulles en retournant la première valeur non nulle

Ces fonctions sont utilisées dans les opérations `groupBy().agg()` pour calculer des statistiques sur nos données, comme le nombre de commandes, le nombre de clients uniques, ou les revenus totaux.

---

## Slide 14 : Concepts Spark - Connexion JDBC

**Texte à dire :**

Spark se connecte à PostgreSQL via JDBC, qui est un standard pour la connexion aux bases de données.

Pour la lecture, on utilise : `spark.read.jdbc(url, table, properties)`. Cela permet de lire une table entière depuis PostgreSQL dans un DataFrame Spark.

Pour l'écriture, on utilise : `df.write.jdbc(url, table, properties)`. Cela permet d'écrire un DataFrame Spark dans une table PostgreSQL.

L'avantage de cette approche est que Spark peut lire et écrire de grandes quantités de données de manière distribuée, en parallélisant les opérations sur plusieurs partitions.

---

## Slide 15 : Flux de données - Import

**Texte à dire :**

Maintenant, je vais vous présenter les flux de données dans notre système.

Pour l'import, le flux est simple : **Fichiers CSV/JSON → Spark DataFrame → PostgreSQL**

Concrètement :

-   Pour les clients : le fichier `customers.csv` est lu par Spark, transformé en DataFrame, puis écrit dans la table `customers`
-   Pour les commandes : les fichiers `orders_YYYY-MM-DD.json` sont lus, transformés en DataFrames, puis écrits dans les tables `orders` et `order_items`
-   Pour les remboursements : le fichier `refunds.csv` est lu, filtré pour valider les références, puis écrit dans la table `refunds`

---

## Slide 16 : Flux de données - Traitement

**Texte à dire :**

Pour le traitement, le flux est : **PostgreSQL → Spark DataFrame → Transformations → PostgreSQL/CSV**

Concrètement :

-   Pour le résumé CSV : on lit les tables PostgreSQL, on effectue des agrégations avec Spark, puis on exporte dans un fichier CSV
-   Pour `orders_clean` : on lit les tables, on agrège par commande, puis on écrit dans la table `orders_clean`
-   Pour `daily_city_sales` : on lit les tables, on agrège par ville et canal, puis on écrit dans la table `daily_city_sales`

Dans tous les cas, Spark effectue les transformations et agrégations de manière distribuée avant d'écrire les résultats.

---

## Slide 17 : Avantages de Spark

**Texte à dire :**

Pourquoi avons-nous choisi Spark ? Voici les principaux avantages :

1. **Performance** : Le traitement distribué permet de gérer de grandes quantités de données efficacement
2. **Scalabilité** : Spark peut gérer des millions de lignes sans problème, en distribuant le travail sur plusieurs cœurs ou machines
3. **Optimisation automatique** : Spark optimise automatiquement les opérations grâce à son catalyseur d'optimisation, qui réécrit les requêtes pour améliorer les performances
4. **API unifiée** : La même API est utilisée pour les fichiers et les bases de données, ce qui simplifie le code
5. **Lazy evaluation** : Les opérations sont optimisées avant l'exécution, ce qui permet à Spark de créer un plan d'exécution optimal

---

## Slide 18 : Gestion des erreurs

**Texte à dire :**

Les deux classes gèrent les erreurs de manière similaire et robuste.

Chaque méthode importante est entourée d'un bloc try/except pour capturer les erreurs. Les messages d'erreur sont explicites pour faciliter le débogage.

En cas d'erreur fatale, les exceptions sont propagées pour arrêter le processus, évitant ainsi la corruption des données. Cette approche garantit l'intégrité des données dans la base.

---

## Slide 19 : Conclusion

**Texte à dire :**

Pour conclure, les classes `DataImporter` et `DataProcessor` utilisent Apache Spark pour :

-   **Importer** efficacement de grandes quantités de données depuis des fichiers vers PostgreSQL
-   **Traiter** et **agréger** ces données de manière distribuée
-   **Exporter** les résultats vers PostgreSQL ou des fichiers CSV

Cette architecture nous permet de gérer des volumes de données importants tout en maintenant de bonnes performances grâce au traitement distribué de Spark.

Le système est scalable, robuste, et peut facilement s'adapter à des volumes de données croissants.

---

## Slide 20 : Questions

**Texte à dire :**

Merci pour votre attention. J'ai terminé ma présentation sur les classes `DataImporter` et `DataProcessor` et leur utilisation d'Apache Spark.

Avez-vous des questions ?
