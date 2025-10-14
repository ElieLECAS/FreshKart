# Partie 3 - Import FreshKart vers PostgreSQL avec génération CSV

## Vue d'ensemble

Cette partie du projet FreshKart importe les données depuis les fichiers CSV/JSON vers PostgreSQL et génère automatiquement un fichier CSV de résumé quotidien, similaire à la Partie 2 mais avec l'intégration complète des tables customers et refunds.

## Fonctionnalités

### 🔄 **Processus d'import avec fichiers temporaires**

1. **Copie** : Les fichiers de `data/input` sont copiés vers `temp/`
2. **Import** : Les données sont importées vers PostgreSQL depuis les fichiers temporaires
3. **Génération CSV** : Un fichier de résumé quotidien est généré depuis PostgreSQL
4. **Tables agrégées** : Création des tables `orders_clean` et `daily_city_sales`
5. **Nettoyage** : Les fichiers temporaires sont supprimés

### 📊 **Génération de CSV de sortie**

-   Format identique à la Partie 2 : `daily_summary_YYYYMMDD.csv`
-   Date cible fixe : **15 mars 2025** (configurable via `TARGET_DATE`)
-   Données enrichies avec les tables `customers` et `refunds`
-   Colonnes : date, city, channel, orders_count, unique_customers, items_sold, gross_revenue_eur, refunds_eur, net_revenue_eur
-   Séparateur : point-virgule (`;`)
-   Encodage : UTF-8

### 🗄️ **Structure de base de données**

-   **customers** : Informations clients (nouveau)
-   **orders** : Commandes avec canal et statut de paiement
-   **order_items** : Articles des commandes avec quantité et prix
-   **refunds** : Remboursements par commande (nouveau)
-   **orders_clean** : Commandes agrégées et nettoyées (équivalent Partie 2)
-   **daily_city_sales** : Résumés quotidiens par ville et canal (équivalent Partie 2)

## Structure des dossiers

```
Partie_3/
├── temp/                    # Dossier temporaire (créé automatiquement)
├── output/                  # Dossier de sortie (créé automatiquement)
│   └── daily_summary_YYYYMMDD.csv
├── import_data.py           # Script principal
├── docker-compose.yml       # Configuration Docker
├── Dockerfile              # Image Docker
└── requirements.txt        # Dépendances Python
```

## Utilisation

### Démarrage complet

```bash
cd Partie_3
docker-compose up
```

### Import d'un fichier spécifique

```bash
docker-compose run python python /app/import_data.py /app/temp/orders_2025-03-15.json
```

## Configuration Docker

### Volumes mappés

-   `../data:/data` : Accès aux fichiers de données originaux
-   `./temp:/app/temp` : Dossier temporaire pour la copie des fichiers
-   `./output:/app/output` : Dossier de sortie pour les CSV générés

### Services

-   **postgres** : Base de données PostgreSQL 15
-   **python** : Conteneur Python avec le script d'import

## Variables d'environnement

Créer un fichier `.env` avec :

```env
POSTGRES_DB=freshkart
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

## Avantages par rapport à la Partie 2

1. **Intégration complète** : Utilise toutes les tables (customers, orders, order_items, refunds)
2. **Base relationnelle** : PostgreSQL au lieu de SQLite
3. **Requêtes SQL avancées** : Utilise des CTE (Common Table Expressions) pour des calculs complexes
4. **Sécurité des données** : Les fichiers originaux ne sont jamais modifiés
5. **Nettoyage automatique** : Pas d'accumulation de fichiers temporaires
6. **Traçabilité** : Logs détaillés de chaque étape

## Format de sortie CSV

Le fichier généré suit exactement le même format que la Partie 2 :

```csv
date;city;channel;orders_count;unique_customers;items_sold;gross_revenue_eur;refunds_eur;net_revenue_eur
2025-03-15;Bordeaux;app;8;8;70;816.9;-39.05;777.85
2025-03-15;Bordeaux;web;6;6;50;479.7;-45.62;434.08
...
```

## Logs et débogage

Le script affiche des logs détaillés pour chaque étape :

-   📁 Copie des fichiers
-   🏗️ Création des tables
-   📋 Import des clients
-   📦 Import des commandes
-   💰 Import des remboursements
-   📊 Génération du CSV
-   🧹 Nettoyage

En cas d'erreur, le nettoyage est effectué automatiquement pour éviter l'accumulation de fichiers temporaires.
