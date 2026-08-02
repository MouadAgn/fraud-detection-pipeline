# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Chaîne en flux : Auto Loader et Structured Streaming
# MAGIC
# MAGIC **Objectif** : traiter les transactions au fil de leur arrivée plutôt que par
# MAGIC lots quotidiens.
# MAGIC
# MAGIC ## Ce que le streaming change réellement
# MAGIC
# MAGIC L'API est presque identique : `readStream` au lieu de `read`, `writeStream`
# MAGIC au lieu de `write`. C'est l'argument commercial de Spark, et il est en partie
# MAGIC trompeur — **la ressemblance du code masque une différence de nature**.
# MAGIC
# MAGIC En batch, chaque traitement voit **toutes** les données. En flux, il ne voit
# MAGIC qu'un micro-lot, et doit se souvenir du reste. Trois conséquences :
# MAGIC
# MAGIC | | Batch | Flux |
# MAGIC |---|---|---|
# MAGIC | Vision des données | Complète | Un micro-lot à la fois |
# MAGIC | Classement global | Possible | **Impossible** — le prochain lot pourrait contenir une ligne antérieure |
# MAGIC | Historique d'un client | Recalculable | Doit être **conservé en état** |
# MAGIC | Reprise après panne | Relancer le traitement | Rejouer depuis le **point de contrôle** |
# MAGIC
# MAGIC Ce notebook traite ces trois difficultés explicitement, plutôt que de les
# MAGIC contourner en silence.
# MAGIC
# MAGIC ## Les tables de flux sont distinctes des tables batch
# MAGIC
# MAGIC `bronze_transactions_stream` et `silver_transactions_stream` coexistent avec
# MAGIC leurs équivalents batch. C'est délibéré : les deux chaînes doivent pouvoir
# MAGIC être **comparées**. Écraser les tables batch interdirait toute vérification
# MAGIC que le streaming produit bien le même résultat.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.getcwd())
SRC_PATH = os.path.join(REPO_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from pyspark.sql import functions as F

from fraud import config, quality, schemas, silver

dbutils.widgets.text("stream_seconds", "180", "Durée d'écoute du flux (s)")
STREAM_SECONDS = int(dbutils.widgets.get("stream_seconds"))

print(config.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Emplacements de flux
# MAGIC
# MAGIC Les volumes sont créés ici comme dans le notebook de rejeu, et pour la même
# MAGIC raison qu'à l'étape 1 : un traitement doit pouvoir s'exécuter sur un
# MAGIC environnement vierge, sans supposer qu'un autre notebook a été lancé avant
# MAGIC lui. La création étant idempotente, la redondance ne coûte rien.

# COMMAND ----------

for volume, purpose in [
    (config.LANDING_VOLUME, "Fichiers de transactions déposés au fil de l'eau"),
    (config.CHECKPOINT_VOLUME, "Points de reprise des traitements en flux"),
]:
    # Apostrophes doublées pour l'échappement SQL — cf. notebook 06.
    comment = purpose.replace("'", "''")
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {config.CATALOG}.{config.SCHEMA}.{volume}
        COMMENT '{comment}'
    """)

print(f"Atterrissage      : {config.LANDING_PATH}")
print(f"Points de reprise : {config.CHECKPOINT_PATH}")

pending = [f for f in dbutils.fs.ls(config.LANDING_PATH) if f.name.endswith(".csv")]
print(f"\nFichiers en attente : {len(pending)}")

if not pending:
    print()
    print("Aucun fichier à traiter. Deux façons de procéder :")
    print("  - lancer 06_streaming_replay en parallèle, dans un autre onglet,")
    print("    pour voir les fichiers consommés au fur et à mesure de leur dépôt ;")
    print("  - ou l'exécuter d'abord en entier, puis revenir ici.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze en flux — Auto Loader
# MAGIC
# MAGIC ### Ce qu'Auto Loader apporte
# MAGIC
# MAGIC Un `readStream` classique sur un répertoire doit **relister tous les
# MAGIC fichiers** à chaque micro-lot pour repérer les nouveaux. Sur un répertoire
# MAGIC qui accumule des millions de fichiers, ce listage devient le goulot
# MAGIC d'étranglement.
# MAGIC
# MAGIC Auto Loader (`cloudFiles`) tient un index des fichiers déjà vus dans son
# MAGIC point de contrôle. Le coût de détection devient proportionnel au nombre de
# MAGIC **nouveaux** fichiers, non au total accumulé.
# MAGIC
# MAGIC ### Le schéma reste explicite
# MAGIC
# MAGIC Même principe qu'en batch : pas d'inférence, toutes les colonnes métier en
# MAGIC texte. L'argument est encore plus fort en flux — une inférence de schéma
# MAGIC dépendrait des premiers fichiers arrivés, et le schéma changerait donc selon
# MAGIC l'ordre d'arrivée.

# COMMAND ----------

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{config.CHECKPOINT_PATH}/bronze_schema")
    .option("header", "true")
    .option("quote", '"')
    .option("escape", '"')
    .schema(schemas.BRONZE_SOURCE_SCHEMA)
    .load(config.LANDING_PATH)
    # Mêmes colonnes de traçabilité qu'en batch. `current_timestamp()` est ici
    # légitime, contrairement au batch : en flux, chaque ligne est réellement
    # ingérée à un instant différent — il n'y a pas de « lot » à horodater
    # d'un seul tenant.
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_name"))
    .withColumn("_ingestion_id", F.lit("stream"))
)

bronze_query = (
    bronze_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/bronze")
    # Un micro-lot toutes les 10 secondes. En production on ajusterait selon la
    # latence acceptable : plus l'intervalle est court, plus la latence est
    # faible et plus le surcoût de coordination est élevé.
    .trigger(processingTime="10 seconds")
    .toTable(config.BRONZE_STREAM_TABLE)
)

print(f"Flux Bronze démarré — écriture vers {config.BRONZE_STREAM_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver en flux
# MAGIC
# MAGIC ### La règle de déduplication ne se transpose pas
# MAGIC
# MAGIC C'est la principale difficulté de cette étape, et elle est instructive.
# MAGIC
# MAGIC En batch, les doublons sont écartés par `row_number()` sur une fenêtre
# MAGIC ordonnée. Cette opération suppose un **classement global** de toutes les
# MAGIC lignes partageant un identifiant. En flux, cette information n'existe pas :
# MAGIC le prochain micro-lot pourrait contenir un exemplaire qui aurait dû arriver
# MAGIC en premier. Spark refuse donc l'opération.
# MAGIC
# MAGIC L'équivalent en flux est `dropDuplicatesWithinWatermark`, qui déduplique sur
# MAGIC une **fenêtre temporelle bornée** au lieu de l'historique complet.
# MAGIC
# MAGIC ### Pourquoi une borne est nécessaire
# MAGIC
# MAGIC Sans elle, Spark devrait mémoriser **tous** les identifiants déjà vus, pour
# MAGIC toujours. L'état croîtrait indéfiniment jusqu'à saturation de la mémoire.
# MAGIC
# MAGIC Le filigrane (*watermark*) fixe la limite : ici, une transaction ne peut
# MAGIC arriver avec plus d'un jour de retard. Au-delà, elle est ignorée et son
# MAGIC identifiant peut être oublié.
# MAGIC
# MAGIC **C'est un compromis assumé, pas une équivalence.** Un doublon séparé de plus
# MAGIC d'un jour passerait en flux alors qu'il serait détecté en batch. La fenêtre
# MAGIC se choisit d'après le retard réellement observé sur les données amont.

# COMMAND ----------

WATERMARK_DELAY = "1 day"

validated_stream = silver.add_validation_helpers(
    spark.readStream.table(config.BRONZE_STREAM_TABLE),
    deduplicate=False,  # voir explication ci-dessus
)

deduplicated = validated_stream.withWatermark(
    silver.PARSED_EVENT_TIME, WATERMARK_DELAY
).dropDuplicatesWithinWatermark(["trans_num"])

# Les règles de rejet portant sur une seule ligne s'appliquent telles quelles.
# Seule celle du doublon est retirée : elle est remplacée par l'opération
# ci-dessus. Le reste du code métier est rigoureusement identique au batch —
# c'est le vrai bénéfice de l'API unifiée.
streaming_rules = [
    rule for rule in silver.reject_rules() if rule.name != "transaction_id_duplicated"
]

clean_stream, _ = quality.split_on_quality(deduplicated, streaming_rules)
flagged_stream = quality.apply_flags(clean_stream, silver.flag_rules())
silver_stream = silver.build_silver_projection(flagged_stream, config.PSEUDONYM_SALT)

silver_query = (
    silver_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/silver")
    .trigger(processingTime="15 seconds")
    .toTable(config.SILVER_STREAM_TABLE)
)

print(f"Flux Silver démarré — écriture vers {config.SILVER_STREAM_TABLE}")
print(f"Règles appliquées : {len(streaming_rules)} sur {len(silver.reject_rules())}")
print("  (la règle de doublon est remplacée par dropDuplicatesWithinWatermark)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Écoute du flux
# MAGIC
# MAGIC Les deux flux tournent en tâche de fond. On les laisse consommer les
# MAGIC fichiers déposés par le notebook `06_streaming_replay`, puis on les arrête
# MAGIC proprement.
# MAGIC
# MAGIC > **Un flux ne s'arrête pas tout seul.** En production il tournerait
# MAGIC > indéfiniment sur un job dédié. Ici, une durée d'écoute bornée évite de
# MAGIC > consommer inutilement le quota de calcul.

# COMMAND ----------

import time

start = time.time()
while time.time() - start < STREAM_SECONDS:
    bronze_rows = bronze_query.lastProgress["numInputRows"] if bronze_query.lastProgress else 0
    silver_rows = silver_query.lastProgress["numInputRows"] if silver_query.lastProgress else 0
    elapsed = int(time.time() - start)
    print(f"  [{elapsed:>3}s] Bronze +{bronze_rows}  Silver +{silver_rows}", flush=True)
    time.sleep(15)

for name, query in [("Bronze", bronze_query), ("Silver", silver_query)]:
    query.stop()
    print(f"Flux {name} arrêté.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vérification — le flux produit-il le même résultat que le batch ?
# MAGIC
# MAGIC C'est la question qui compte. Une chaîne temps réel qui produirait des
# MAGIC résultats différents du batch serait inexploitable : on ne saurait plus
# MAGIC laquelle croire.

# COMMAND ----------

bronze_count = spark.table(config.BRONZE_STREAM_TABLE).count()
silver_count = spark.table(config.SILVER_STREAM_TABLE).count()

print(f"Bronze en flux : {bronze_count:,} lignes".replace(",", " "))
print(f"Silver en flux : {silver_count:,} lignes".replace(",", " "))
print(f"Taux de rejet  : {100 * (bronze_count - silver_count) / max(bronze_count, 1):.4f} %")

# Comparaison ligne à ligne avec la chaîne batch, sur les transactions communes.
# L'identifiant pseudonymisé et le montant doivent être rigoureusement
# identiques : la même logique métier, appliquée aux mêmes données, doit
# produire le même résultat quel que soit le mode d'exécution.
comparison = (
    spark.table(config.SILVER_STREAM_TABLE).alias("flux")
    .join(
        spark.table(config.SILVER_TABLE).alias("batch"),
        on="transaction_id",
        how="inner",
    )
    .select(
        F.count("*").alias("transactions_communes"),
        F.sum(F.when(F.col("flux.customer_id") != F.col("batch.customer_id"), 1).otherwise(0)).alias("ecarts_customer_id"),
        F.sum(F.when(F.col("flux.amount") != F.col("batch.amount"), 1).otherwise(0)).alias("ecarts_montant"),
        F.sum(F.when(F.col("flux.customer_zip") != F.col("batch.customer_zip"), 1).otherwise(0)).alias("ecarts_code_postal"),
    )
)
display(comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ce qui ne passe PAS en flux : les variables comportementales
# MAGIC
# MAGIC C'est la limite réelle de cette étape, et la plus intéressante à expliquer.
# MAGIC
# MAGIC Les variables de la couche Gold reposent sur des fenêtres glissantes par
# MAGIC porteur — « combien de transactions dans la dernière heure », « quel est son
# MAGIC montant moyen historique ». En batch, Spark relit tout l'historique. En flux,
# MAGIC il faudrait **conserver un état par client**, indéfiniment.
# MAGIC
# MAGIC Deux architectures existent :
# MAGIC
# MAGIC | Approche | Principe | Limite |
# MAGIC |---|---|---|
# MAGIC | **État Spark** (`flatMapGroupsWithState`) | Spark garde l'historique de chaque client en mémoire entre les micro-lots | État proportionnel au nombre de clients ; complexité et coût de reprise élevés |
# MAGIC | **Profil précalculé + jointure flux/statique** | Le batch calcule chaque nuit un profil par client ; le flux le consulte | Le profil a jusqu'à 24 h de retard |
# MAGIC
# MAGIC **La seconde a été retenue.** C'est l'architecture la plus répandue en
# MAGIC production, parce qu'elle sépare ce qui doit être temps réel — la décision —
# MAGIC de ce qui peut ne pas l'être — la connaissance du comportement habituel d'un
# MAGIC client, qui n'évolue pas d'une minute à l'autre.
# MAGIC
# MAGIC C'est le principe d'un *feature store*.

# COMMAND ----------

# Profil comportemental par porteur, calculé en batch depuis la table Silver.
customer_profile = (
    spark.table(config.SILVER_TABLE)
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("profile_txn_count"),
        F.avg("amount").alias("profile_amount_mean"),
        F.stddev("amount").alias("profile_amount_std"),
        F.max("amount").alias("profile_amount_max"),
        F.max("event_time").alias("profile_computed_until"),
    )
)

(
    customer_profile.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.CUSTOMER_PROFILE_TABLE)
)

spark.sql(f"""
    COMMENT ON TABLE {config.CUSTOMER_PROFILE_TABLE} IS
    'Profil comportemental par porteur, recalculé en batch et consulté par la
     chaîne en flux. Sépare ce qui doit être temps réel — la décision — de ce
     qui peut ne pas l''être : le comportement habituel d''un client n''évolue
     pas d''une minute à l''autre.'
""")

print(f"Profil écrit : {config.CUSTOMER_PROFILE_TABLE} "
      f"({spark.table(config.CUSTOMER_PROFILE_TABLE).count()} porteurs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jointure flux / statique
# MAGIC
# MAGIC Chaque transaction du flux est enrichie de l'écart au comportement habituel
# MAGIC de son porteur, sans qu'aucun état n'ait à être conservé par Spark.
# MAGIC
# MAGIC Cette jointure est possible parce que le côté statique est **petit** — 999
# MAGIC porteurs — et donc diffusable à tous les exécuteurs. Sur des millions de
# MAGIC clients, il faudrait un magasin clé-valeur à faible latence plutôt qu'une
# MAGIC table Delta.

# COMMAND ----------

enriched = (
    spark.readStream.table(config.SILVER_STREAM_TABLE)
    .join(spark.table(config.CUSTOMER_PROFILE_TABLE), on="customer_id", how="left")
    .withColumn(
        "amount_zscore_vs_profile",
        F.when(
            F.col("profile_amount_std") > 0,
            (F.col("amount") - F.col("profile_amount_mean")) / F.col("profile_amount_std"),
        ),
    )
    .withColumn("hour_of_day", F.hour("event_time"))
    .withColumn("is_night", F.hour("event_time").isin([22, 23, 0, 1, 2, 3]))
    .select(
        "transaction_id", "customer_id", "event_time", "amount",
        "merchant_category", "amount_zscore_vs_profile", "hour_of_day",
        "is_night", "is_fraud",
    )
)

enrichment_query = (
    enriched.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/enriched")
    .trigger(availableNow=True)  # traite ce qui est disponible puis s'arrête
    .toTable(config.SCORED_STREAM_TABLE)
)
enrichment_query.awaitTermination()

print(f"Table enrichie : {config.SCORED_STREAM_TABLE} "
      f"({spark.table(config.SCORED_STREAM_TABLE).count():,} lignes)".replace(",", " "))

display(
    spark.table(config.SCORED_STREAM_TABLE)
    .orderBy(F.col("amount_zscore_vs_profile").desc_nulls_last())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Ingestion | Auto Loader, détection incrémentale des fichiers |
# MAGIC | Bronze en flux | `bronze_transactions_stream` |
# MAGIC | Silver en flux | `silver_transactions_stream` — 8 règles sur 9 |
# MAGIC | Déduplication | `dropDuplicatesWithinWatermark`, fenêtre d'un jour |
# MAGIC | Variables comportementales | Profil batch + jointure flux/statique |
# MAGIC | Reprise | Points de contrôle par flux |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Points à savoir expliquer en soutenance
# MAGIC
# MAGIC 1. **Le code batch ne se transpose pas tel quel.** `row_number()` suppose un
# MAGIC    classement global que le flux ne peut pas fournir. La ressemblance des
# MAGIC    API masque une différence de nature : en batch on voit tout, en flux on
# MAGIC    voit un micro-lot et on doit se souvenir du reste.
# MAGIC 2. **Le filigrane borne l'état.** Sans lui, Spark mémoriserait tous les
# MAGIC    identifiants vus, pour toujours. C'est un compromis assumé : un doublon
# MAGIC    séparé de plus d'un jour passerait en flux alors qu'il serait détecté en
# MAGIC    batch.
# MAGIC 3. **Auto Loader n'est pas un `readStream` sur répertoire.** Il tient un
# MAGIC    index des fichiers vus : le coût de détection dépend du nombre de
# MAGIC    nouveaux fichiers, non du total accumulé.
# MAGIC 4. **Les variables comportementales ne passent pas en flux pur.** Un profil
# MAGIC    précalculé en batch, consulté par jointure flux/statique, sépare ce qui
# MAGIC    doit être temps réel — la décision — de ce qui peut ne pas l'être. C'est
# MAGIC    le principe d'un *feature store*.
# MAGIC 5. **Le point de contrôle est ce qui rend un flux redémarrable.** Le vider
# MAGIC    fait tout réingérer ; ne pas le vider après avoir remplacé les fichiers
# MAGIC    donne un flux qui ne traite rien, sans erreur.
