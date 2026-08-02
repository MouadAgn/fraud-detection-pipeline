# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Simulateur de flux
# MAGIC
# MAGIC **Objectif** : rejouer des transactions sous forme de fichiers déposés au fil
# MAGIC de l'eau, comme le ferait un système amont.
# MAGIC
# MAGIC ## Pourquoi simuler plutôt que brancher Kafka
# MAGIC
# MAGIC Le corpus est un fichier statique. Pour démontrer une chaîne temps réel, il
# MAGIC faut donc produire l'arrivée continue qui n'existe pas dans les données.
# MAGIC
# MAGIC Deux voies étaient possibles :
# MAGIC
# MAGIC | Approche | Ce qu'elle démontre | Coût |
# MAGIC |---|---|---|
# MAGIC | **Fichiers déposés + Auto Loader** | Ingestion incrémentale, reprise sur point de contrôle, évolution de schéma | Aucun service externe |
# MAGIC | Kafka | Idem, plus la gestion d'un courtier de messages | Un cluster Kafka à administrer |
# MAGIC
# MAGIC Le dépôt de fichiers a été retenu : il correspond au cas le plus fréquent en
# MAGIC banque — un système transactionnel qui exporte périodiquement — et il isole
# MAGIC ce qui est réellement en jeu, à savoir le **traitement incrémental**, sans le
# MAGIC mêler à l'exploitation d'un courtier.
# MAGIC
# MAGIC ## Le format d'atterrissage est un contrat, pas un héritage
# MAGIC
# MAGIC Les fichiers déposés portent les **noms de colonnes du pipeline**, et non la
# MAGIC première colonne anonyme du CSV historique. La différence est délibérée : le
# MAGIC format d'atterrissage est une interface qu'on définit, alors que le fichier
# MAGIC d'origine était un existant qu'il fallait subir.

# COMMAND ----------

import os
import sys
import time

REPO_ROOT = os.path.dirname(os.getcwd())
SRC_PATH = os.path.join(REPO_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from fraud import config, schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Création des emplacements de flux
# MAGIC
# MAGIC | Volume | Rôle |
# MAGIC |---|---|
# MAGIC | `landing` | Répertoire surveillé par Auto Loader |
# MAGIC | `checkpoints` | Points de reprise des flux |
# MAGIC
# MAGIC Le point de contrôle mérite une explication : c'est lui qui rend un flux
# MAGIC **redémarrable**. Il mémorise les fichiers déjà ingérés et la position
# MAGIC atteinte. Sans lui, un redémarrage réingérerait tout depuis le début et
# MAGIC produirait des doublons.

# COMMAND ----------

for volume, purpose in [
    (config.LANDING_VOLUME, "Fichiers de transactions déposés au fil de l'eau"),
    (config.CHECKPOINT_VOLUME, "Points de reprise des traitements en flux"),
]:
    # Les apostrophes doivent être doublées pour être échappées en SQL : sans
    # cela, « au fil de l'eau » referme la chaîne après « l » et le reste
    # devient une instruction invalide. Le français en contient beaucoup.
    comment = purpose.replace("'", "''")
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {config.CATALOG}.{config.SCHEMA}.{volume}
        COMMENT '{comment}'
    """)

print(f"Atterrissage      : {config.LANDING_PATH}")
print(f"Points de reprise : {config.CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Paramètres du rejeu
# MAGIC
# MAGIC On rejoue les transactions les plus récentes du corpus, découpées en lots
# MAGIC déposés à intervalle régulier.

# COMMAND ----------

dbutils.widgets.text("replay_rows", "20000", "Transactions à rejouer")
dbutils.widgets.text("batches", "10", "Nombre de fichiers")
dbutils.widgets.text("delay_seconds", "5", "Délai entre deux dépôts (s)")
dbutils.widgets.dropdown("reset", "true", ["true", "false"], "Vider l'atterrissage")

REPLAY_ROWS = int(dbutils.widgets.get("replay_rows"))
BATCHES = int(dbutils.widgets.get("batches"))
DELAY_SECONDS = int(dbutils.widgets.get("delay_seconds"))
RESET = dbutils.widgets.get("reset") == "true"

print(f"{REPLAY_ROWS:,} transactions en {BATCHES} fichiers, "
      f"un dépôt toutes les {DELAY_SECONDS} s".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Remise à zéro
# MAGIC
# MAGIC Vider l'atterrissage **et** les points de reprise, sinon Auto Loader
# MAGIC considérerait les fichiers réécrits comme déjà traités et n'ingérerait rien —
# MAGIC une confusion classique quand on rejoue une démonstration.

# COMMAND ----------

if RESET:
    for path in (config.LANDING_PATH, config.CHECKPOINT_PATH):
        try:
            for entry in dbutils.fs.ls(path):
                dbutils.fs.rm(entry.path, recurse=True)
            print(f"Vidé : {path}")
        except Exception as error:  # volume vide ou inexistant
            print(f"Rien à vider dans {path} ({type(error).__name__})")

    for table in (config.BRONZE_STREAM_TABLE, config.SILVER_STREAM_TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"Table supprimée : {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Préparation des lots
# MAGIC
# MAGIC La source du rejeu est la table Bronze : elle contient les colonnes telles
# MAGIC qu'elles ont été reçues, en texte, ce qui reproduit fidèlement ce qu'un
# MAGIC système amont déposerait.
# MAGIC
# MAGIC Les transactions sont découpées en lots **par ordre chronologique** :
# MAGIC rejouer dans le désordre produirait des délais négatifs entre transactions
# MAGIC et n'aurait aucun sens métier.

# COMMAND ----------

# Le découpage en lots est matérialisé dans une table intermédiaire plutôt que
# mis en cache mémoire : `.cache()` n'est pas supporté sur le compute serverless.
#
# Ce n'est pas un simple contournement. Sans matérialisation, le plan complet —
# lecture de 1,85 M de lignes, tri, limite, découpage — serait réexécuté à
# chaque écriture de fichier, soit dix fois. La table intermédiaire ne pèse que
# quelques milliers de lignes et rend les dépôts instantanés.
#
# L'avertissement « No Partition Defined for Window operation » est attendu :
# `ntile` numérote les lots sur l'ensemble trié, ce qui suppose effectivement
# une partition unique. Sur 20 000 lignes, c'est sans conséquence.
STAGING_TABLE = config.fqn("tmp_streaming_replay")
chronological = Window.orderBy(F.col("trans_date_trans_time").asc())

(
    spark.table(config.BRONZE_TABLE)
    .select(*schemas.SOURCE_COLUMN_ORDER)
    .orderBy(F.col("trans_date_trans_time").desc())
    .limit(REPLAY_ROWS)
    .withColumn("_batch", F.ntile(BATCHES).over(chronological))
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(STAGING_TABLE)
)

batches = spark.table(STAGING_TABLE)
total = batches.count()
print(f"{total:,} transactions préparées en {BATCHES} lots "
      f"(~{total // BATCHES:,} par lot)".replace(",", " "))

display(
    batches.groupBy("_batch")
    .agg(
        F.count("*").alias("transactions"),
        F.min("trans_date_trans_time").alias("debut"),
        F.max("trans_date_trans_time").alias("fin"),
    )
    .orderBy("_batch")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dépôt progressif
# MAGIC
# MAGIC Chaque itération écrit un fichier dans l'atterrissage, puis attend. C'est
# MAGIC cette attente qui simule l'arrivée continue : sans elle, les dix fichiers
# MAGIC apparaîtraient simultanément et Auto Loader les traiterait en un seul
# MAGIC micro-lot, ce qui ne démontrerait rien.
# MAGIC
# MAGIC > **Lance le notebook `07_streaming_pipeline` pendant que cette cellule
# MAGIC > tourne.** Tu verras les fichiers être consommés au fur et à mesure de leur
# MAGIC > dépôt — c'est la démonstration elle-même.

# COMMAND ----------

for index in range(1, BATCHES + 1):
    (
        batches.filter(F.col("_batch") == index)
        .drop("_batch")
        .coalesce(1)  # un seul fichier par lot, pour que le dépôt soit lisible
        .write.mode("append")
        .option("header", "true")
        .csv(config.LANDING_PATH)
    )
    print(f"  Lot {index}/{BATCHES} déposé", flush=True)

    if index < BATCHES:
        time.sleep(DELAY_SECONDS)

files = [f for f in dbutils.fs.ls(config.LANDING_PATH) if f.name.endswith(".csv")]
print()
print(f"Dépôt terminé : {len(files)} fichiers dans l'atterrissage.")

# La table intermédiaire n'a plus d'utilité : la laisser encombrerait le schéma
# et laisserait croire qu'elle fait partie du modèle de données.
spark.sql(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
print(f"Table intermédiaire supprimée : {STAGING_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC Les fichiers sont dans `/Volumes/workspace/fraud_detection/landing/`.
# MAGIC
# MAGIC **Point à savoir expliquer** : pourquoi vider les points de reprise lors
# MAGIC d'une remise à zéro ? Parce qu'Auto Loader y mémorise les fichiers déjà
# MAGIC ingérés. Vider l'atterrissage sans vider les points de reprise donne un flux
# MAGIC qui ne traite rien et n'émet aucune erreur — le symptôme le plus déroutant
# MAGIC du streaming, et la première chose à vérifier quand « rien ne se passe ».
# MAGIC
# MAGIC **Étape suivante** : `07_streaming_pipeline`.
