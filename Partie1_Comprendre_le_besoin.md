# Partie 1 : Comprendre le besoin et cadrer

## 1. Identification des sources de données

### Sources de données identifiées

D'après le cahier des charges FreshKart, les sources de données sont :

1. **`customers.csv`** : Fichier CSV contenant les informations clients

    - Structure : `customer_id`, `first_name`, `last_name`, `email`, `city`, `is_active`
    - Volume : 800 clients (802 lignes avec header)
    - Format : CSV avec séparateur virgule, encodage UTF-8

2. **`orders_YYYY-MM-DD.json`** : Fichiers JSON quotidiens des commandes

    - Structure : Tableau JSON contenant des objets commandes
    - Chaque commande : `order_id`, `customer_id`, `channel`, `created_at`, `payment_status`, `items[]`
    - Chaque item : `sku`, `qty`, `unit_price`
    - Volume : 31 fichiers (mars 2025), ~3100 commandes, ~7761 items
    - Format : JSON, un fichier par jour

3. **`refunds.csv`** : Fichier CSV des remboursements
    - Structure : `refund_id`, `order_id`, `amount`, `reason`, `created_at`
    - Volume : 1122 remboursements
    - Format : CSV avec montants négatifs

## 2. Transformations nécessaires

### Filtres à appliquer

1. **Filtrage des commandes** : Conserver uniquement `payment_status = 'paid'`
2. **Filtrage des clients** : Exclure les clients inactifs (`is_active = false`)
3. **Filtrage des prix** : Écarter les lignes avec prix unitaire négatif
4. **Déduplication** : Sur `order_id` (garder données les plus récentes lors du rechargement)

**Note :** Dans l'implémentation actuelle, le pipeline importe 100% des données sans filtrage métier. Les filtres ci-dessus correspondent aux règles métier du cahier des charges, mais peuvent être appliqués lors des requêtes d'analyse plutôt qu'à l'import.

### Jointures nécessaires

1. **Orders ↔ Customers** : Via `customer_id` pour récupérer la ville du client
2. **Orders ↔ Refunds** : Via `order_id` pour agréger les remboursements
3. **Orders ↔ OrderItems** : Via `order_id` pour calculer les totaux

### Agrégations requises

1. **Par date, ville et canal** :

    - Nombre de commandes (`orders_count`)
    - Nombre de clients uniques (`unique_customers`)
    - Nombre d'items vendus (`items_sold`)
    - Chiffre d'affaires brut (`gross_revenue_eur`)

2. **Agrégation des remboursements** :
    - Somme des montants par commande (valeurs négatives)
    - Chiffre d'affaires net = brut + remboursements

### Calculs spécifiques

1. **Conversion monétaire** : Utiliser l'euro (€) comme séparateur décimal
2. **Prix total par item** : `qty × unit_price`
3. **Revenu brut** : Somme des prix totaux par commande
4. **Revenu net** : Revenu brut + remboursements (négatifs)

## 3. Veille sur les sources de données en entreprise

### Panorama des sources de données

#### 1. **Fichiers plats (CSV, JSON, XML)**

**Avantages :**

-   ✅ Simplicité de lecture et écriture
-   ✅ Compatibilité universelle
-   ✅ Pas de dépendance à un SGBD
-   ✅ Facile à déboguer et valider
-   ✅ Support natif par la plupart des outils d'analyse

**Inconvénients :**

-   ❌ Pas de contraintes d'intégrité
-   ❌ Pas de relations entre données
-   ❌ Performance limitée sur gros volumes
-   ❌ Pas de concurrence d'accès
-   ❌ Pas de transactionnalité

**Cas d'usage :** Échange de données, export/import, données temporaires

#### 2. **SGBD relationnels (PostgreSQL, MySQL, Oracle)**

**Avantages :**

-   ✅ Intégrité référentielle (clés étrangères)
-   ✅ Transactions ACID
-   ✅ Gestion de la concurrence
-   ✅ Requêtes SQL puissantes
-   ✅ Indexation pour les performances
-   ✅ Sauvegarde et récupération

**Inconvénients :**

-   ❌ Complexité de mise en place
-   ❌ Nécessite des compétences DBA
-   ❌ Coût des licences (certains SGBD)
-   ❌ Latence réseau (si distant)

**Cas d'usage :** Applications métier, données transactionnelles, analytics

#### 3. **APIs REST/SOAP**

**Avantages :**

-   ✅ Données en temps réel
-   ✅ Sécurité et authentification
-   ✅ Versioning des données
-   ✅ Documentation standardisée
-   ✅ Intégration facile avec les applications

**Inconvénients :**

-   ❌ Dépendance au service externe
-   ❌ Limites de débit (rate limiting)
-   ❌ Latence réseau
-   ❌ Complexité de gestion des erreurs
-   ❌ Évolution des formats de données

**Cas d'usage :** Intégrations tierces, données externes, microservices

#### 4. **Bases NoSQL (MongoDB, Cassandra, Redis)**

**Avantages :**

-   ✅ Flexibilité du schéma
-   ✅ Performance sur gros volumes
-   ✅ Scaling horizontal
-   ✅ Types de données variés (JSON, graph, clé-valeur)

**Inconvénients :**

-   ❌ Pas de relations complexes
-   ❌ Pas de garanties ACID (certains)
-   ❌ Courbe d'apprentissage
-   ❌ Moins d'outils d'analyse

**Cas d'usage :** Big Data, données non structurées, applications web modernes

#### 5. **Data Lakes / Data Warehouses (S3, Snowflake, BigQuery)**

**Avantages :**

-   ✅ Stockage massif et économique
-   ✅ Analytics avancées
-   ✅ Intégration multi-sources
-   ✅ Machine Learning intégré

**Inconvénients :**

-   ❌ Complexité architecturale
-   ❌ Coûts variables
-   ❌ Nécessite des compétences spécialisées
-   ❌ Latence pour les requêtes complexes

**Cas d'usage :** Business Intelligence, Machine Learning, données historiques

### Recommandations pour FreshKart

**Pour le contexte actuel :**

-   **Fichiers CSV/JSON** : Appropriés pour l'import initial et les échanges
-   **PostgreSQL** : Idéal pour le stockage final et les requêtes analytiques
-   **SQLite** : Bon compromis pour les tests et développements locaux

**Évolution future :**

-   Migration vers un Data Warehouse pour les analyses avancées
-   APIs pour l'intégration avec d'autres systèmes
-   Streaming de données en temps réel pour les tableaux de bord

## 4. Architecture de données recommandée

### Modèle relationnel proposé

```
customers (customer_id PK, first_name, last_name, email, city, is_active)
    ↓
orders (order_id PK, customer_id FK, channel, created_at, payment_status)
    ↓
order_items (id PK, order_id FK, sku, qty, unit_price)
    ↓
refunds (refund_id PK, order_id FK, amount, reason, created_at)
```

### Livrables attendus

1. **Fichier CSV quotidien** : `daily_summary_YYYYMMDD.csv`

    - Colonnes : `date;city;channel;orders_count;unique_customers;items_sold;gross_revenue_eur;refunds_eur;net_revenue_eur`
    - Format : Séparateur `;`, encodage UTF-8

2. **Base de données PostgreSQL** :
    - Tables : `orders_clean`, `daily_city_sales`
    - Relations et contraintes d'intégrité
    - Index pour les performances

Cette architecture permet une normalisation correcte des données tout en préservant l'historique des prix et en facilitant les analyses futures.
