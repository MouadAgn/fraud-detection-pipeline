# Dictionnaire de données

> **Document généré** — ne pas modifier à la main.
> Source de vérité : `SILVER_DATA_DICTIONARY` dans [`src/fraud/silver.py`](../src/fraud/silver.py).
> Régénérer avec `py -3.13 scripts/generate_data_dictionary.py`.

*Dernière génération : 2026-07-29*

---

## Pourquoi ce document

Les noms de colonnes du pipeline sont en **anglais** : c'est le standard professionnel, et c'est aussi la langue des colonnes de la source. La documentation fonctionnelle est en **français**, langue du mémoire.

Ces mêmes descriptions sont posées comme commentaires sur les colonnes dans Unity Catalog : elles s'affichent directement dans le Catalog Explorer de Databricks, au plus près de la donnée.

Au-delà de la question de la langue, un dictionnaire de données est un livrable attendu en contexte bancaire réglementé : il documente la signification et la provenance de chaque champ, pour les auditeurs comme pour les analystes.

---

## Table `silver_transactions`

26 colonnes.

| Colonne | Type | Origine | Description |
| --- | --- | --- | --- |
| `transaction_id` | STRING | `trans_num` | Identifiant unique de la transaction (empreinte de 32 caractères issue de la source). |
| `event_time` | TIMESTAMP | `trans_date_trans_time` | Horodatage de la transaction. Référence temporelle unique du pipeline : toutes les fenêtres, agrégations et découpages train/test s'appuient sur cette colonne. |
| `event_date` | DATE | `dérivée de event_time` | Date de la transaction. Facilite les agrégations quotidiennes du tableau de bord. |
| `event_time_unix` | BIGINT | `dérivée de event_time` | Horodatage en secondes depuis le 1er janvier 1970. Requis par les fenêtres glissantes de la couche Gold, Spark ne sachant borner un rangeBetween que sur une valeur numérique. Remplace la colonne unix_time de la source, décalée de 7 ans. |
| `customer_id` | STRING | `cc_num (pseudonymisé)` | Identifiant pseudonymisé du porteur de carte : empreinte SHA-256 salée du numéro de carte. Déterministe, donc stable entre exécutions et entre les périodes d'entraînement et de test. |
| `customer_gender` | STRING | `gender` | Genre déclaré du porteur (F ou M). |
| `customer_age` | INT | `dérivée de dob et event_time` | Âge du porteur en années révolues au moment de la transaction. La date de naissance elle-même n'est pas conservée (minimisation des données). |
| `customer_job` | STRING | `job` | Profession déclarée du porteur. |
| `customer_city` | STRING | `city` | Ville de résidence du porteur. |
| `customer_state` | STRING | `state` | État américain de résidence du porteur (code à deux lettres). |
| `customer_zip` | STRING | `zip (réparé)` | Code postal du porteur, sur 5 chiffres. Le zéro de tête, détruit en amont par un traitement numérique, est restauré par le pipeline (6,28 % des lignes concernées). |
| `customer_city_population` | INT | `city_pop` | Population de la ville de résidence du porteur. |
| `customer_lat` | DOUBLE | `lat` | Latitude du domicile du porteur. |
| `customer_lon` | DOUBLE | `long` | Longitude du domicile du porteur. Renommée depuis long, mot réservé en SQL. |
| `merchant_name` | STRING | `merchant (préfixe retiré)` | Raison sociale du commerçant. Le préfixe technique « fraud_ », présent sur la totalité des commerçants de la source, a été retiré après vérification qu'il n'est pas corrélé à la variable cible. |
| `merchant_category` | STRING | `category` | Catégorie d'achat du commerçant (14 modalités : alimentaire, carburant, santé, voyage, etc.). |
| `merchant_lat` | DOUBLE | `merch_lat` | Latitude du point de vente. |
| `merchant_lon` | DOUBLE | `merch_long` | Longitude du point de vente. |
| `amount` | DOUBLE | `amt` | Montant de la transaction, en dollars américains. |
| `is_fraud` | INT | `is_fraud` | Variable cible. Vaut 1 si la transaction est frauduleuse, 0 sinon. Classe positive fortement minoritaire (0,52 % du corpus). |
| `flag_minor_cardholder` | BOOLEAN | `dérivée` | Indicateur de qualité : le porteur avait moins de 18 ans au moment de la transaction. La ligne est conservée — un mineur peut légitimement être porteur secondaire d'une carte. |
| `flag_zip_code_repaired` | BOOLEAN | `dérivée` | Indicateur de qualité : le code postal reçu ne comportait que 4 caractères et son zéro de tête a été restauré par le pipeline. |
| `_ingested_at` | TIMESTAMP | `Bronze` | Horodatage d'entrée de la ligne dans la plateforme (couche Bronze). |
| `_source_file` | STRING | `Bronze` | Nom du fichier source dont la ligne est issue. |
| `_ingestion_id` | STRING | `Bronze` | Identifiant du lot d'ingestion ayant produit la ligne. |
| `_silver_processed_at` | TIMESTAMP | `dérivée` | Horodatage du traitement de nettoyage et de mise en conformité (couche Silver). |

---

## Colonnes de Bronze non reprises en Silver

Toute suppression est une décision, et toute décision se justifie.

| Colonne source | Motif de suppression |
| --- | --- |
| `source_row_index` | index technique de l'export CSV, sans valeur métier |
| `first` | identifiant direct — prénom du porteur (RGPD) |
| `last` | identifiant direct — nom du porteur (RGPD) |
| `street` | identifiant direct — adresse postale précise (RGPD) |
| `cc_num` | remplacé par customer_id, pseudonymisé |
| `dob` | donnée personnelle — remplacée par customer_age, non identifiant |
| `unix_time` | incohérent avec l'horodatage (décalage de 7 ans) — recalculé |

---

## Conventions

- Les colonnes préfixées **`_`** portent la traçabilité technique (ingestion, traitement) et non de la donnée métier.
- Les colonnes préfixées **`flag_`** sont des indicateurs de qualité : elles signalent une anomalie sans écarter la ligne.
- Les colonnes préfixées **`customer_`** et **`merchant_`** lèvent l'ambiguïté de la source, où `lat`/`long` et `merch_lat`/`merch_long` désignaient deux entités différentes sans le dire.
