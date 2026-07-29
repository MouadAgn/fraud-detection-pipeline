# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Couche Gold : variables comportementales
# MAGIC
# MAGIC **Objectif** : construire les variables explicatives du modèle à partir de
# MAGIC l'historique de chaque porteur, sans jamais y injecter d'information
# MAGIC postérieure à la transaction.
# MAGIC
# MAGIC ## La contrainte qui gouverne toute l'étape : pas de fuite temporelle
# MAGIC
# MAGIC Une variable comme « montant moyen historique du client » se calcule sur une
# MAGIC fenêtre. Si cette fenêtre inclut la transaction courante, on injecte dans les
# MAGIC variables explicatives une information **qui n'existait pas au moment de la
# MAGIC décision**.
# MAGIC
# MAGIC Le piège est redoutable parce qu'il est **silencieux** : le modèle affiche
# MAGIC alors des performances excellentes, et rien dans les métriques ne le signale.
# MAGIC L'erreur ne se révèle qu'en production, quand le modèle s'effondre.
# MAGIC
# MAGIC Toutes les fenêtres sont donc bornées à `-1` : strictement les transactions
# MAGIC **antérieures** du même client. Et plutôt que de l'affirmer, ce notebook en
# MAGIC apporte la **preuve formelle** (section 5).
# MAGIC
# MAGIC ## Pourquoi les variables sont calculées avant le découpage train/test
# MAGIC
# MAGIC C'est une question que le jury peut poser, et la réponse est contre-intuitive.
# MAGIC
# MAGIC Les variables sont calculées sur **l'historique complet et continu**, train et
# MAGIC test confondus. Ce n'est **pas** une fuite : chaque ligne ne voit que son
# MAGIC propre passé, et le passé d'une transaction de test inclut légitimement des
# MAGIC transactions de la période d'entraînement — c'est exactement ce qui se
# MAGIC produirait en production.
# MAGIC
# MAGIC Découper d'abord aurait au contraire créé un **artefact** : les premières
# MAGIC transactions du jeu de test auraient perdu tout leur historique et se
# MAGIC seraient présentées comme des clients inconnus, ce qu'elles ne sont pas.
# MAGIC
# MAGIC L'ordre correct est donc : **variables → découpage → SMOTE**.
# MAGIC
# MAGIC ## Deux familles de fenêtres
# MAGIC
# MAGIC | | Bornage | Question posée | Usage |
# MAGIC |---|---|---|---|
# MAGIC | `rangeBetween(-N, -1)` | sur la **valeur** du temps | « combien dans la dernière heure ? » | Vélocité |
# MAGIC | `rowsBetween(unbounded, -1)` | sur le **nombre de lignes** | « quel est son comportement habituel ? » | Historique |
# MAGIC
# MAGIC `rangeBetween` n'accepte qu'une seule colonne de tri : les fenêtres de
# MAGIC vélocité trient sur le seul horodatage, tandis que les fenêtres d'historique
# MAGIC ajoutent l'identifiant de transaction comme critère de départage — sans quoi
# MAGIC deux transactions à la même seconde seraient ordonnées arbitrairement et le
# MAGIC résultat varierait d'une exécution à l'autre.

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

from pyspark.sql import functions as F

from fraud import config, features, silver

print(config.describe())

# COMMAND ----------

silver_df = spark.table(config.SILVER_TABLE)
silver_count = silver_df.count()

print(f"Lignes lues depuis Silver : {silver_count:,}")
print(f"Porteurs distincts        : {silver_df.select('customer_id').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Les variables construites
# MAGIC
# MAGIC ### Vélocité — le scénario de rafale
# MAGIC
# MAGIC `txn_count_1h` · `txn_count_24h` · `txn_count_7d` · `amount_sum_1h` ·
# MAGIC `amount_sum_24h` · `seconds_since_previous_txn`
# MAGIC
# MAGIC Les largeurs de fenêtre ne sont pas conventionnelles : elles sont justifiées
# MAGIC par la cadence mesurée au profilage. Un porteur légitime transige toutes les
# MAGIC **4,6 h** en médiane, alors que **43,3 % des fraudes** surviennent moins d'une
# MAGIC heure après la précédente sur la même carte. C'est à l'échelle de l'heure que
# MAGIC le contraste est maximal.
# MAGIC
# MAGIC ### Écart au comportement habituel — le cœur du dispositif
# MAGIC
# MAGIC `customer_amount_mean_hist` · `customer_amount_std_hist` ·
# MAGIC **`amount_zscore_vs_history`** · `amount_ratio_vs_mean`
# MAGIC
# MAGIC C'est la famille la plus directement liée à la problématique. Un seuil absolu
# MAGIC — « au-delà de 500 $, c'est suspect » — produit mécaniquement des **faux
# MAGIC positifs chez les gros dépensiers** et laisse passer les fraudes chez les
# MAGIC petits. Une mesure relative au client répond à « inhabituel pour *lui* », qui
# MAGIC est la question métier réelle.
# MAGIC
# MAGIC ### Nouveauté
# MAGIC
# MAGIC `is_new_merchant_for_customer` · `is_new_category_for_customer` ·
# MAGIC `customer_category_share_hist`
# MAGIC
# MAGIC La variable ne dit pas « ce commerçant est suspect » mais « ce client n'y
# MAGIC était jamais allé » — nuance qui évite de pénaliser les petits commerçants.
# MAGIC
# MAGIC ### Temporel
# MAGIC
# MAGIC `hour_of_day` · `day_of_week` · `is_weekend` · `is_night`
# MAGIC
# MAGIC ### Écartée sur preuve
# MAGIC
# MAGIC La **distance géographique** porteur/commerçant, préconisée dans la
# MAGIC méthodologie initiale, a été mesurée puis écartée : 76,11 km en moyenne pour
# MAGIC les transactions légitimes contre 76,27 km pour les fraudes — distributions
# MAGIC indiscernables (note 01, constat 5).
# MAGIC
# MAGIC ### Valeurs manquantes : une absence n'est pas un zéro
# MAGIC
# MAGIC | Cas | Valeur | Pourquoi |
# MAGIC |---|---|---|
# MAGIC | Compteur sur fenêtre vide | `0` | « aucune transaction dans l'heure » est une **mesure** |
# MAGIC | Moyenne sans historique | `NULL` | le montant habituel d'un client sans passé est **indéterminé**, pas nul |
# MAGIC
# MAGIC Imputer `0` à la moyenne affirmerait au modèle que ce client dépense
# MAGIC habituellement 0 $. XGBoost traite nativement les valeurs manquantes en
# MAGIC apprenant, pour chaque nœud, la direction par défaut la plus favorable : mieux
# MAGIC vaut lui transmettre l'absence que la déguiser.

# COMMAND ----------

gold = features.build_gold_features(silver_df)

new_columns = [c for c in gold.columns if c not in silver_df.columns]
print(f"Variables créées : {len(new_columns)}")
for column in new_columns:
    print(f"  {column}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sélection des colonnes de la table Gold
# MAGIC
# MAGIC Les colonnes intermédiaires de calcul sont écartées, ainsi que les colonnes
# MAGIC de Silver sans usage en aval (géolocalisation, ville, profession, indicateurs
# MAGIC de qualité). La table Gold reste ainsi lisible et centrée sur ce qui alimente
# MAGIC le modèle.

# COMMAND ----------

gold_columns = (
    list(features.PASSTHROUGH_COLUMNS)
    + [c for c in features.GOLD_DATA_DICTIONARY if c != "_gold_processed_at"]
    + ["_gold_processed_at"]
)

gold_df = gold.select(*gold_columns)

print(f"Colonnes de la table Gold : {len(gold_df.columns)}")
print(f"Dont variables explicatives : {len(features.FEATURE_COLUMNS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Écriture de la table Gold

# COMMAND ----------

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.GOLD_FEATURES_TABLE)
)

spark.sql(f"""
    COMMENT ON TABLE {config.GOLD_FEATURES_TABLE} IS
    'Couche Gold — variables comportementales prêtes pour la modélisation.
     Toutes les fenêtres sont bornées aux transactions strictement antérieures
     du même porteur : aucune variable ne contient d''information postérieure à
     la transaction qu''elle décrit. Les variables sont calculées sur
     l''historique complet et continu, avant tout découpage train/test.'
""")

gold_table = spark.table(config.GOLD_FEATURES_TABLE)
gold_count = gold_table.count()

print(f"Table écrite : {config.GOLD_FEATURES_TABLE}")
print(f"Lignes : {gold_count:,}  (Silver : {silver_count:,})")

if gold_count != silver_count:
    raise ValueError(
        f"Le feature engineering a modifié le nombre de lignes : "
        f"{silver_count:,} en entrée, {gold_count:,} en sortie."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Preuve formelle de l'absence de fuite temporelle
# MAGIC
# MAGIC C'est la section la plus importante du notebook.
# MAGIC
# MAGIC Affirmer « les fenêtres sont bien bornées » n'a aucune valeur : c'est
# MAGIC exactement ce que dirait quelqu'un qui s'est trompé. Ces trois contrôles
# MAGIC vérifient une **propriété mathématique du résultat**, indépendamment du code
# MAGIC qui l'a produit.
# MAGIC
# MAGIC | Contrôle | Propriété vérifiée |
# MAGIC |---|---|
# MAGIC | `history_count_mismatch` | Pour la k-ième transaction d'un porteur, `customer_txn_count_hist` doit valoir **exactement k−1**. Toute autre valeur prouve que la fenêtre ne contient pas précisément les lignes antérieures. |
# MAGIC | `first_txn_has_history` | La première transaction d'un porteur ne peut pas avoir de montant moyen historique. |
# MAGIC | `velocity_not_monotonic` | `txn_count_1h ≤ txn_count_24h ≤ txn_count_7d ≤ historique`. Une inversion trahirait une fenêtre mal bornée. |
# MAGIC
# MAGIC Le premier contrôle est le plus fort : il ne se contente pas de détecter une
# MAGIC inclusion de la ligne courante, il vérifie que la fenêtre contient **ni plus
# MAGIC ni moins** que les transactions antérieures.

# COMMAND ----------

checks = features.leakage_check_expressions()

audited = gold_table
for name, expression in checks.items():
    audited = audited.withColumn(name, expression)

violations = audited.agg(
    *[F.sum(F.when(F.col(name), 1).otherwise(0)).alias(name) for name in checks]
).collect()[0]

print(f"{'Contrôle anti-fuite':<28}{'Violations':>12}   Statut")
print("-" * 56)
leaks = []
for name in checks:
    count = int(violations[name] or 0)
    print(f"{name:<28}{count:>12,}   {'OK' if count == 0 else 'FUITE DETECTEE'}")
    if count:
        leaks.append(f"{name} : {count:,} lignes")

if leaks:
    raise ValueError(
        "Fuite temporelle détectée — les variables sont inexploitables :\n"
        + "\n".join(f"  - {leak}" for leak in leaks)
    )

print()
print("Aucune fuite temporelle : les fenêtres ne contiennent que le passé.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Illustration : la preuve se lit à l'œil nu
# MAGIC
# MAGIC Les huit premières transactions d'un porteur. Trois lectures à faire :
# MAGIC
# MAGIC 1. `customer_txn_count_hist` vaut **0, 1, 2, 3…** — la k-ième transaction voit
# MAGIC    exactement k−1 antécédents.
# MAGIC 2. Sur la première ligne, `customer_amount_mean_hist` est **vide** : aucun
# MAGIC    historique n'existe encore.
# MAGIC 3. Sur chaque ligne, `customer_amount_mean_hist` est la moyenne des `amount`
# MAGIC    des lignes **au-dessus**, jamais de la ligne courante — vérifiable
# MAGIC    mentalement sur les deux ou trois premières.

# COMMAND ----------

sample_customer = (
    gold_table.groupBy("customer_id")
    .count()
    .orderBy(F.col("count").desc())
    .first()["customer_id"]
)

display(
    gold_table.filter(F.col("customer_id") == sample_customer)
    .orderBy("event_time_unix", "transaction_id")
    .select(
        "event_time",
        "amount",
        "customer_txn_count_hist",
        "customer_amount_mean_hist",
        "amount_zscore_vs_history",
        "txn_count_24h",
        "is_new_merchant_for_customer",
    )
    .limit(8)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pouvoir discriminant des variables
# MAGIC
# MAGIC Comparaison de la moyenne de chaque variable numérique entre transactions
# MAGIC légitimes et frauduleuses. Ce tableau ne remplace pas l'analyse SHAP de
# MAGIC l'étape 4, mais il donne dès maintenant une indication : une variable dont le
# MAGIC rapport est proche de 1 n'apporte probablement rien.
# MAGIC
# MAGIC C'est aussi ce qui permettra de justifier, en Partie 2, que les variables
# MAGIC retenues l'ont été **sur mesure** et non par principe.

# COMMAND ----------

NUMERIC_FEATURES = [
    "amount",
    "txn_count_1h",
    "txn_count_24h",
    "txn_count_7d",
    "amount_sum_1h",
    "amount_sum_24h",
    "amount_sum_7d",
    "seconds_since_previous_txn",
    "customer_txn_count_hist",
    "customer_amount_mean_hist",
    "customer_amount_std_hist",
    "amount_zscore_vs_history",
    "amount_ratio_vs_mean",
    "customer_category_share_hist",
    "hour_of_day",
    "customer_age",
]

stats = (
    gold_table.groupBy("is_fraud")
    .agg(*[F.avg(c).alias(c) for c in NUMERIC_FEATURES])
    .orderBy("is_fraud")
    .collect()
)
legit, fraud = stats[0], stats[1]

rows = []
for column in NUMERIC_FEATURES:
    a, b = legit[column], fraud[column]
    ratio = (b / a) if (a not in (None, 0) and b is not None) else None
    rows.append((column, a, b, ratio))
rows.sort(key=lambda r: abs((r[3] or 1) - 1), reverse=True)

print(f"{'Variable':<30}{'Légitime':>14}{'Fraude':>14}{'Rapport':>10}")
print("-" * 68)
for column, a, b, ratio in rows:
    ratio_text = f"x{ratio:.2f}" if ratio else "-"
    print(f"{column:<30}{a:>14.3f}{b:>14.3f}{ratio_text:>10}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Taux de valeurs manquantes
# MAGIC
# MAGIC Attendu et normal : les premières transactions de chaque porteur n'ont pas
# MAGIC d'historique. Avec 999 porteurs sur 1,85 M de transactions, le taux doit
# MAGIC rester marginal — s'il était élevé, cela signalerait une erreur de
# MAGIC partitionnement.

# COMMAND ----------

nullable = [
    "seconds_since_previous_txn",
    "customer_amount_mean_hist",
    "customer_amount_std_hist",
    "amount_zscore_vs_history",
    "amount_ratio_vs_mean",
    "customer_category_share_hist",
]
missing = gold_table.select(
    *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in nullable]
).collect()[0]

print(f"{'Variable':<32}{'Manquantes':>12}{'Taux':>10}")
print("-" * 54)
for column in nullable:
    n = int(missing[column] or 0)
    print(f"{column:<32}{n:>12,}{100 * n / gold_count:>9.4f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Dictionnaire de données de la couche Gold
# MAGIC
# MAGIC Comme en Silver : noms techniques en anglais, description métier en français,
# MAGIC posée sur chaque colonne dans Unity Catalog. Les colonnes reprises de Silver
# MAGIC conservent leur description d'origine.

# COMMAND ----------

comments = {
    **{
        column: silver.SILVER_COLUMN_COMMENTS[column]
        for column in features.PASSTHROUGH_COLUMNS
        if column in silver.SILVER_COLUMN_COMMENTS
    },
    **features.GOLD_COLUMN_COMMENTS,
}

for statement in silver.column_comment_statements(config.GOLD_FEATURES_TABLE, comments):
    spark.sql(statement)

print(f"{len(comments)} colonnes documentées en français.")

display(
    spark.sql(f"DESCRIBE TABLE {config.GOLD_FEATURES_TABLE}")
    .filter(F.col("col_name").isNotNull() & (F.col("col_name") != ""))
    .select("col_name", "data_type", "comment")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Table produite | `workspace.fraud_detection.gold_features` |
# MAGIC | Lignes | 1 852 394 — inchangé depuis Silver |
# MAGIC | Variables comportementales créées | 20 |
# MAGIC | Variables explicatives soumises au modèle | 26 |
# MAGIC | Contrôles anti-fuite | 3, tous à 0 violation |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Points à savoir expliquer en soutenance
# MAGIC
# MAGIC 1. **Qu'est-ce qu'une fuite temporelle, et comment l'avez-vous évitée ?**
# MAGIC    Toutes les fenêtres sont bornées à `-1`. Et ce n'est pas affirmé : le
# MAGIC    contrôle `history_count_mismatch` vérifie que la k-ième transaction d'un
# MAGIC    porteur voit exactement k−1 antécédents — ni plus, ni moins.
# MAGIC 2. **Pourquoi calculer les variables avant le découpage train/test ?** Parce
# MAGIC    que chaque ligne ne voit que son propre passé. Découper d'abord aurait
# MAGIC    amputé les premières transactions du test de leur historique et les aurait
# MAGIC    fait passer pour des clients inconnus — un artefact, pas une précaution.
# MAGIC 3. **Pourquoi un z-score plutôt qu'un seuil de montant ?** Un seuil absolu
# MAGIC    génère des faux positifs chez les gros dépensiers et rate les fraudes chez
# MAGIC    les petits. Le z-score mesure l'inhabituel **relatif au client** — c'est la
# MAGIC    réponse directe à la problématique des faux positifs.
# MAGIC 4. **Pourquoi laisser des valeurs manquantes ?** Parce qu'un client sans
# MAGIC    historique n'a pas un montant habituel de 0 $ : il n'en a pas. XGBoost
# MAGIC    apprend une direction par défaut pour les valeurs manquantes ; les imputer
# MAGIC    reviendrait à lui transmettre une information fausse.
# MAGIC 5. **Pourquoi `merchant_name` n'est-il pas une variable ?** 693 modalités :
# MAGIC    l'encoder produirait un modèle qui mémorise des commerçants au lieu
# MAGIC    d'apprendre des comportements. C'est `is_new_merchant_for_customer` qui
# MAGIC    porte l'information utile.
# MAGIC
# MAGIC **Étape suivante** : `04_model_training` — découpage temporel, SMOTE sur le
# MAGIC train uniquement, comparaison de modèles, optimisation du seuil de décision.
