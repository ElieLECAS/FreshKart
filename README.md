# Pipeline FreshKart PostgreSQL

Pipeline de consolidation des données FreshKart avec Docker Compose, PostgreSQL et Python.

## Architecture

-   **PostgreSQL 15** : Base de données pour stocker toutes les données
-   **Python 3.11** : Script d'import avec SQLAlchemy
-   **Docker Compose** : Orchestration des services

## Structure des données

### Tables créées

1. **`customers`** : Tous les clients

    - `customer_id` (PK), `first_name`, `last_name`, `email`, `city`, `is_active`

2. **`orders`** : Toutes les commandes

    - `order_id` (PK), `customer_id` (FK), `channel`, `created_at`, `payment_status`

3. **`order_items`** : Items de chaque commande

    - `id` (PK auto), `order_id` (FK), `sku`, `qty`, `unit_price`

4. **`refunds`** : Tous les remboursements
    - `refund_id` (PK), `order_id` (FK), `amount`, `reason`, `created_at`

## Installation et utilisation

### Prérequis

-   Docker et Docker Compose installés

### Lancement

1. **Démarrer PostgreSQL** :

    ```bash
    docker-compose up -d postgres
    ```

2. **Exécuter l'import des données** :

    **Mode complet (tous les fichiers)** :

    ```bash
    docker-compose run --rm python
    ```

    **Mode fichier spécifique** :

    ```bash
    docker-compose run --rm python python /app/import_data.py /data/input/orders_2025-03-15.json
    ```

### Accès à la base de données

Une fois PostgreSQL démarré, vous pouvez vous connecter :

```bash
# Via Docker
docker-compose exec postgres psql -U postgres -d freshkart

# Ou depuis l'extérieur
psql -h localhost -p 5432 -U postgres -d freshkart
```

## Données importées

Le script importe **100% des données** sans filtrage :

-   ✅ **customers.csv** : Tous les clients (actifs et inactifs) - importé une seule fois
-   ✅ **orders\_\*.json** : Toutes les commandes (paid, pending, etc.) - par date
-   ✅ **order_items** : Tous les items (même avec prix négatif) - par date
-   ✅ **refunds.csv** : Tous les remboursements - importé une seule fois
-   ✅ **Déduplication** : Sur `order_id` (première occurrence conservée)
-   ✅ **Gestion par date** : Suppression/rechargement des données par date spécifique

## Fonctionnalités

-   **Gestion par date** : Suppression/rechargement des données par date spécifique
-   **Import intelligent** : Clients et remboursements importés une seule fois
-   **Gestion d'erreurs** : Rollback automatique en cas d'erreur
-   **Logs détaillés** : Affichage du progrès et des statistiques
-   **Relations** : Clés étrangères et relations SQLAlchemy configurées
-   **Mode flexible** : Import complet ou fichier spécifique

## Exemples de requêtes

```sql
-- Compter les commandes par statut
SELECT payment_status, COUNT(*)
FROM orders
GROUP BY payment_status;

-- Voir les commandes avec leurs items
SELECT o.order_id, o.channel, oi.sku, oi.qty, oi.unit_price
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
LIMIT 10;

-- Revenus par ville
SELECT c.city, SUM(oi.qty * oi.unit_price) as revenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.city
ORDER BY revenue DESC;
```
