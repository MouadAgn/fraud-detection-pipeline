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

Transactions typées, contrôlées et pseudonymisées. 26 colonnes.

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

## Table `gold_features`

Variables comportementales prêtes pour la modélisation. Reprend les colonnes de contexte de `silver_transactions` et y ajoute les 21 colonnes ci-dessous.

> **Toutes les fenêtres sont bornées aux transactions strictement antérieures du même porteur.** Aucune variable ne contient d'information postérieure à la transaction qu'elle décrit — propriété vérifiée par trois contrôles automatiques dans le notebook `03_gold_features`.

| Colonne | Type | Origine | Description |
| --- | --- | --- | --- |
| `txn_count_1h` | BIGINT | `fenêtre 1 h` | Nombre de transactions du porteur dans l'heure précédant celle-ci, transaction courante exclue. Largeur choisie sur mesure : 43,3 % des fraudes surviennent moins d'une heure après la précédente, contre un rythme normal d'une transaction toutes les 4,6 heures. |
| `txn_count_24h` | BIGINT | `fenêtre 24 h` | Nombre de transactions du porteur dans les 24 heures précédentes, transaction courante exclue. Couvre un cycle d'activité complet. |
| `txn_count_7d` | BIGINT | `fenêtre 7 j` | Nombre de transactions du porteur dans les 7 jours précédents, transaction courante exclue. Sert de référence d'activité récente. |
| `amount_sum_1h` | DOUBLE | `fenêtre 1 h` | Montant total dépensé par le porteur dans l'heure précédente. Vaut 0 en l'absence de transaction sur la période. |
| `amount_sum_24h` | DOUBLE | `fenêtre 24 h` | Montant total dépensé par le porteur dans les 24 heures précédentes. Vaut 0 en l'absence de transaction sur la période. |
| `amount_sum_7d` | DOUBLE | `fenêtre 7 j` | Montant total dépensé par le porteur dans les 7 jours précédents. Vaut 0 en l'absence de transaction sur la période. |
| `seconds_since_previous_txn` | BIGINT | `dérivée` | Délai en secondes depuis la transaction précédente du même porteur. Non renseigné pour sa première transaction. |
| `customer_txn_count_hist` | BIGINT | `historique complet` | Nombre total de transactions antérieures du porteur. Mesure la profondeur d'historique disponible : les variables comportementales sont peu fiables quand elle est faible. |
| `customer_amount_mean_hist` | DOUBLE | `historique complet` | Montant moyen des transactions antérieures du porteur. Non renseigné pour sa première transaction — un client sans historique n'a pas un montant habituel nul, il n'en a pas. |
| `customer_amount_std_hist` | DOUBLE | `historique complet` | Écart-type des montants antérieurs du porteur. Non renseigné en deçà de deux transactions. |
| `customer_amount_max_hist` | DOUBLE | `historique complet` | Montant le plus élevé jamais dépensé par le porteur avant cette transaction. |
| `amount_zscore_vs_history` | DOUBLE | `dérivée` | Écart du montant courant à la moyenne du porteur, exprimé en nombre d'écarts-types de son propre historique. Variable centrale du dispositif : elle mesure l'inhabituel relatif au client plutôt qu'un seuil absolu, qui pénaliserait les gros dépensiers et laisserait passer les fraudes chez les petits. |
| `amount_ratio_vs_mean` | DOUBLE | `dérivée` | Rapport entre le montant courant et le montant moyen historique du porteur. Lecture plus directe que le z-score, et robuste lorsque l'écart-type est très faible. |
| `is_new_merchant_for_customer` | BOOLEAN | `historique (client, commerçant)` | Vrai si le porteur n'a jamais effectué de transaction chez ce commerçant auparavant. Une carte compromise est typiquement utilisée chez des commerçants inconnus du porteur. |
| `is_new_category_for_customer` | BOOLEAN | `historique (client, catégorie)` | Vrai si le porteur n'a jamais effectué d'achat dans cette catégorie auparavant. |
| `customer_category_share_hist` | DOUBLE | `dérivée` | Part de cette catégorie dans l'historique d'achat du porteur. Une valeur proche de zéro signale une catégorie marginale pour lui. |
| `hour_of_day` | INT | `dérivée de event_time` | Heure de la transaction (0 à 23). Signal le plus discriminant du corpus : le taux de fraude est 10 à 30 fois supérieur entre 22 h et 3 h. Cette amplitude tient en partie à la règle de génération du jeu de données simulé, ce qui impose de surveiller son poids dans l'analyse d'explicabilité. |
| `day_of_week` | INT | `dérivée de event_time` | Jour de la semaine (1 = dimanche, 7 = samedi). |
| `is_weekend` | BOOLEAN | `dérivée de event_time` | Vrai si la transaction a lieu un samedi ou un dimanche. |
| `is_night` | BOOLEAN | `dérivée de event_time` | Vrai si la transaction a lieu entre 22 h et 3 h — plage où le taux de fraude observé est le plus élevé. |
| `_gold_processed_at` | TIMESTAMP | `dérivée` | Horodatage du calcul des variables comportementales (couche Gold). |

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
- Les colonnes suffixées **`_hist`** portent sur l'historique **antérieur** du porteur, transaction courante exclue.
- Les colonnes suffixées **`_1h`**, **`_24h`**, **`_7d`** portent sur une fenêtre glissante précédant la transaction, celle-ci exclue.
