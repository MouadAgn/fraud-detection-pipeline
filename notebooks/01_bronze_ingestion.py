# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Couche Bronze : ingestion
# MAGIC
# MAGIC **Objectif** : charger les CSV sources en table Delta, **sans aucune
# MAGIC transformation métier**, en y ajoutant la traçabilité d'ingestion.
# MAGIC
# MAGIC ## La règle de Bronze : fidélité absolue à la source
# MAGIC
# MAGIC Bronze ne corrige rien, ne type rien, ne filtre rien. Même les défauts
# MAGIC connus du fichier source y sont conservés tels quels :
# MAGIC
# MAGIC | Défaut connu (cf. note 01) | Traitement en Bronze |
# MAGIC |---|---|
# MAGIC | `source_row_index` — index technique sans valeur métier | **Conservé** |
# MAGIC | `unix_time` — décalé de 7 ans par rapport à l'horodatage | **Conservé tel quel** |
# MAGIC | Préfixe `fraud_` sur tous les commerçants | **Conservé** |
# MAGIC | Colonnes `first`, `last`, `long` — noms ambigus en SQL | **Conservés** |
# MAGIC
# MAGIC **Pourquoi s'imposer cette discipline ?** Parce que c'est elle qui rend le
# MAGIC pipeline rejouable. Si Bronze corrigeait déjà la donnée, une erreur découverte
# MAGIC dans une règle de nettoyage obligerait à retourner chercher les fichiers
# MAGIC sources — qui peuvent avoir disparu, avoir été écrasés, ou ne plus être
# MAGIC accessibles. Avec un Bronze fidèle, il suffit de relancer Silver.
# MAGIC
# MAGIC Bronze est la **copie de référence** : le point de retour en arrière du
# MAGIC pipeline.
# MAGIC
# MAGIC ## Ce que Bronze ajoute quand même
# MAGIC
# MAGIC Trois colonnes de traçabilité, préfixées `_` pour ne pas être confondues avec
# MAGIC les colonnes métier. Elles n'altèrent pas la donnée : elles la documentent.
# MAGIC
# MAGIC | Colonne | Répond à la question |
# MAGIC |---|---|
# MAGIC | `_ingested_at` | Quand cette ligne est-elle entrée dans la plateforme ? |
# MAGIC | `_source_file` | De quel fichier provient-elle ? |
# MAGIC | `_ingestion_id` | Quel lot d'ingestion l'a produite ? |
# MAGIC
# MAGIC Sans ces colonnes, un écart constaté en aval est indébuggable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.getcwd())
SRC_PATH = os.path.join(REPO_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from datetime import datetime, timezone

from pyspark.sql import functions as F

from fraud import config, schemas

print(config.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contrôle du contrat de données — validation de l'en-tête
# MAGIC
# MAGIC Ce contrôle s'exécute **avant** la lecture, et il n'est pas cosmétique.
# MAGIC
# MAGIC Le schéma explicite est appliqué par **position**, pas par nom. Si la source
# MAGIC changeait l'ordre de deux colonnes de même type — par exemple `lat` et
# MAGIC `long` — Spark chargerait les données sans la moindre erreur, mais toutes
# MAGIC les latitudes seraient des longitudes. Le défaut ne se manifesterait qu'au
# MAGIC niveau des résultats du modèle, des semaines plus tard, et serait
# MAGIC pratiquement impossible à diagnostiquer.
# MAGIC
# MAGIC Comparer l'en-tête réel au contrat déclaré coûte quelques millisecondes et
# MAGIC élimine entièrement ce risque.

# COMMAND ----------

for filename in schemas.EXPECTED_ROW_COUNTS:
    path = f"{config.VOLUME_PATH}/{filename}"
    with open(path, "r", encoding="utf-8") as handle:
        header = handle.readline()
    schemas.validate_raw_header(header)  # lève SchemaValidationError si non conforme
    print(f"OK — en-tête conforme : {filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lecture des fichiers sources
# MAGIC
# MAGIC Chaque option de lecture est un choix, pas un réglage par défaut :
# MAGIC
# MAGIC | Option | Valeur | Justification |
# MAGIC |---|---|---|
# MAGIC | `schema` | explicite, tout en `STRING` | Pas d'inférence : elle coûte une lecture complète du fichier et produit un type instable d'un lot à l'autre. Tout en texte car un CSV n'a pas de types — typer, c'est déjà interpréter, et ce n'est pas le rôle de Bronze. |
# MAGIC | `header` | `true` | La première ligne est un en-tête, elle ne doit pas devenir une ligne de données. |
# MAGIC | `quote` / `escape` | `"` | **Indispensable ici** : des champs contiennent des virgules (`"fraud_Rippin, Kub and Mann"`, `"Psychologist, counselling"`). Sans guillemets correctement interprétés, toutes les colonnes suivantes seraient décalées. |
# MAGIC | `mode` | `FAILFAST` | Une ligne structurellement corrompue doit **arrêter** l'ingestion. En mode `PERMISSIVE` (le défaut), elle entrerait silencieusement avec des valeurs nulles — exactement ce que Bronze doit interdire. |
# MAGIC
# MAGIC Les deux fichiers sont lus **en une seule passe** (`*.csv`) et atterrissent
# MAGIC dans une table unique. La colonne `_source_file` conserve leur origine, ce qui
# MAGIC suffit à les distinguer. Le découpage train/test se fera plus tard sur la
# MAGIC **date**, pas sur le fichier : c'est un choix méthodologique, pas un héritage
# MAGIC de la façon dont Kaggle a distribué les données.

# COMMAND ----------

raw = (
    spark.read.format("csv")
    .schema(schemas.BRONZE_SOURCE_SCHEMA)
    .option("header", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "FAILFAST")
    .load(f"{config.VOLUME_PATH}/*.csv")
)

print(f"Colonnes lues : {len(raw.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ajout de la traçabilité d'ingestion
# MAGIC
# MAGIC L'horodatage est calculé **une seule fois côté Python**, puis appliqué comme
# MAGIC constante à toutes les lignes. On n'utilise volontairement pas
# MAGIC `current_timestamp()` : cette fonction est évaluée au moment de l'exécution
# MAGIC de chaque tâche Spark, si bien que les lignes d'un même lot pourraient porter
# MAGIC des horodatages différents selon la partition qui les traite. Un lot doit
# MAGIC avoir **un** horodatage, sinon la traçabilité devient approximative.
# MAGIC
# MAGIC `_source_file` provient de `_metadata.file_name`, la colonne de métadonnées
# MAGIC fournie par Spark sur toute lecture de fichiers.

# COMMAND ----------

ingested_at = datetime.now(timezone.utc)
ingestion_id = ingested_at.strftime("%Y%m%dT%H%M%SZ")

bronze = (
    raw.withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
    .withColumn("_source_file", F.col("_metadata.file_name"))
    .withColumn("_ingestion_id", F.lit(ingestion_id))
)

print(f"Identifiant du lot : {ingestion_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Écriture en table Delta
# MAGIC
# MAGIC ### Pourquoi `overwrite` et non `append` ?
# MAGIC
# MAGIC La source est un **instantané complet** de deux fichiers statiques, pas un
# MAGIC flux incrémental. Recharger le tout est donc la sémantique correcte, et
# MAGIC `overwrite` garantit l'**idempotence** : exécuter ce notebook deux fois
# MAGIC produit exactement le même résultat. Avec `append`, un second passage
# MAGIC dupliquerait les 1,85 M de lignes.
# MAGIC
# MAGIC L'historique n'est pas perdu pour autant : Delta conserve chaque version de
# MAGIC la table (*time travel*), et `DESCRIBE HISTORY` en donne la trace complète.
# MAGIC
# MAGIC > À l'étape 8 (streaming), Auto Loader remplacera ce chargement complet par
# MAGIC > un traitement incrémental des seuls nouveaux fichiers.
# MAGIC
# MAGIC ### Pourquoi ne pas partitionner la table ?
# MAGIC
# MAGIC Le réflexe serait de partitionner par date. Ce serait une **erreur** ici.
# MAGIC
# MAGIC Le corpus couvre 731 jours pour 1,85 M lignes, soit environ **2 500 lignes par
# MAGIC jour** — quelques centaines de kilo-octets. Partitionner par jour créerait 731
# MAGIC dossiers contenant chacun un fichier minuscule : c'est le *small files
# MAGIC problem*, qui dégrade les performances au lieu de les améliorer, chaque
# MAGIC lecture devant ouvrir des centaines de fichiers pour rassembler quelques
# MAGIC mégaoctets.
# MAGIC
# MAGIC L'ordre de grandeur communément retenu est qu'une partition doit peser **au
# MAGIC moins 1 Go**. Avec ~200 Mo au total après compression Delta, cette table
# MAGIC n'atteint même pas la taille d'**une seule** partition. Elle reste donc non
# MAGIC partitionnée, et Delta gère le regroupement des fichiers automatiquement.

# COMMAND ----------

(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.BRONZE_TABLE)
)

spark.sql(f"""
    COMMENT ON TABLE {config.BRONZE_TABLE} IS
    'Couche Bronze — copie fidèle des CSV Sparkov, sans transformation métier.
     Toutes les colonnes métier sont en STRING : le typage est une responsabilité
     de la couche Silver. Les colonnes préfixées _ portent la traçabilité d''ingestion.'
""")

print(f"Table écrite : {config.BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Contrôles de réconciliation source ↔ cible
# MAGIC
# MAGIC Écrire une table ne prouve pas qu'on a écrit **la bonne** table. Ces contrôles
# MAGIC comparent le résultat aux volumétries mesurées indépendamment lors du
# MAGIC profilage (`scripts/profile_dataset.py`).
# MAGIC
# MAGIC Ce n'est pas un contrôle de confort : un mauvais traitement des guillemets
# MAGIC dans le CSV modifierait à la fois le nombre de lignes et l'alignement des
# MAGIC colonnes. La comparaison des comptages le détecte immédiatement.

# COMMAND ----------

actual = spark.table(config.BRONZE_TABLE)
actual_by_file = {
    row["_source_file"]: row["n"]
    for row in actual.groupBy("_source_file").agg(F.count("*").alias("n")).collect()
}

print(f"{'Fichier':<20} {'Attendu':>12} {'Obtenu':>12}   Statut")
print("-" * 60)

errors = []
for filename, expected in schemas.EXPECTED_ROW_COUNTS.items():
    obtained = actual_by_file.get(filename, 0)
    ok = obtained == expected
    print(f"{filename:<20} {expected:>12,} {obtained:>12,}   {'OK' if ok else 'ECHEC'}")
    if not ok:
        errors.append(f"{filename}: attendu {expected:,}, obtenu {obtained:,}")

total = actual.count()
total_ok = total == schemas.EXPECTED_TOTAL_ROWS
print("-" * 60)
print(f"{'TOTAL':<20} {schemas.EXPECTED_TOTAL_ROWS:>12,} {total:>12,}   {'OK' if total_ok else 'ECHEC'}")
if not total_ok:
    errors.append(f"total: attendu {schemas.EXPECTED_TOTAL_ROWS:,}, obtenu {total:,}")

# La cible est en STRING à ce stade : la comparaison se fait donc sur '1'.
frauds = actual.filter(F.col("is_fraud") == "1").count()
frauds_ok = frauds == schemas.EXPECTED_TOTAL_FRAUDS
print(f"{'Fraudes':<20} {schemas.EXPECTED_TOTAL_FRAUDS:>12,} {frauds:>12,}   {'OK' if frauds_ok else 'ECHEC'}")
if not frauds_ok:
    errors.append(f"fraudes: attendu {schemas.EXPECTED_TOTAL_FRAUDS:,}, obtenu {frauds:,}")

if errors:
    raise ValueError("Réconciliation en échec :\n" + "\n".join(f"  - {e}" for e in errors))

print()
print("Réconciliation complète : la table Bronze est conforme à la source.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Aperçu du résultat

# COMMAND ----------

display(
    spark.table(config.BRONZE_TABLE)
    .select(
        "trans_date_trans_time", "cc_num", "merchant", "category", "amt",
        "is_fraud", "_source_file", "_ingested_at", "_ingestion_id",
    )
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Historique Delta — la preuve du *time travel*
# MAGIC
# MAGIC Chaque écriture crée une version de la table. C'est ce mécanisme qui rend une
# MAGIC évaluation de modèle réellement reproductible : le run MLflow enregistrera le
# MAGIC numéro de version des données sur lesquelles il a été entraîné, et cette
# MAGIC version restera relisible même après plusieurs rechargements.

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {config.BRONZE_TABLE}")
    .select("version", "timestamp", "operation", "operationMetrics")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Table produite | `workspace.fraud_detection.bronze_transactions` |
# MAGIC | Lignes | 1 852 394 |
# MAGIC | Colonnes | 23 métier (toutes `STRING`) + 3 de traçabilité |
# MAGIC | Transformations métier | **aucune** |
# MAGIC | Idempotence | oui — `overwrite` sur une source complète |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Points à savoir expliquer en soutenance
# MAGIC
# MAGIC 1. **Pourquoi un schéma explicite et pas `inferSchema` ?** Coût (double
# MAGIC    lecture du fichier), instabilité (le type dépend du contenu du lot), et
# MAGIC    absence de contrat versionné.
# MAGIC 2. **Pourquoi tout en `STRING` en Bronze ?** Un CSV n'a pas de types. Typer,
# MAGIC    c'est interpréter — et en mode permissif, une valeur non convertible
# MAGIC    devient `NULL` sans erreur, soit une perte de données invisible dans la
# MAGIC    couche censée garantir la fidélité.
# MAGIC 3. **Pourquoi valider l'en-tête ?** Parce que le schéma s'applique par
# MAGIC    position : une inversion de colonnes de même type corromprait les données
# MAGIC    en silence.
# MAGIC 4. **Pourquoi ne pas partitionner ?** ~2 500 lignes par jour : partitionner
# MAGIC    créerait 731 fichiers minuscules (*small files problem*) et dégraderait
# MAGIC    les performances.
# MAGIC 5. **Pourquoi `overwrite` ?** Source complète et statique : c'est ce qui
# MAGIC    garantit l'idempotence. L'historique reste accessible par time travel.
# MAGIC
# MAGIC **Étape suivante** : `02_silver_cleaning` — typage, contrôles qualité et
# MAGIC pseudonymisation RGPD.
