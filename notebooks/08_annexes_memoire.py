# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Production des figures du mémoire
# MAGIC
# MAGIC **Objectif** : produire en un seul passage les figures et tableaux destinés
# MAGIC aux annexes du mémoire.
# MAGIC
# MAGIC Les notebooks du pipeline affichent des tableaux de texte, adaptés au
# MAGIC contrôle mais peu lisibles une fois imprimés. Ce notebook les complète par
# MAGIC des figures conçues pour le papier.
# MAGIC
# MAGIC ## Choix de rendu
# MAGIC
# MAGIC Toutes les figures sont produites sur **fond blanc**, et non sur le thème
# MAGIC sombre de Databricks. Une capture sombre insérée dans un mémoire imprimé
# MAGIC devient illisible et consomme de l'encre inutilement.
# MAGIC
# MAGIC ## Correspondance avec les annexes
# MAGIC
# MAGIC | Section | Annexe |
# MAGIC |---|---|
# MAGIC | 1 | **A1** — Structure du jeu de données |
# MAGIC | 2 | **A2** — Analyse exploratoire (4 figures) |
# MAGIC | 3 | **A5** — Pseudonymisation : cardinalité avant / après |
# MAGIC | 4 | **A6** — Volumétrie Gold et valeurs manquantes |
# MAGIC | 5 | **A9** — Courbe du coût métier |
# MAGIC | 6 | **A10** — Importance des variables (SHAP) |

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.dirname(os.getcwd())
SRC_PATH = os.path.join(REPO_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import shap
from pyspark.sql import functions as F

from fraud import config, features, metrics, modeling

mlflow.set_registry_uri("databricks-uc")

# Rendu clair, lisible à l'impression.
plt.style.use("default")
plt.rcParams.update(
    {
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "figure.dpi": 110,
        "font.size": 11,
        "axes.grid": True,
        "grid.alpha": 0.3,
    }
)

FRAUD_COLOR = "#c0392b"
LEGIT_COLOR = "#2980b9"

print("Environnement prêt.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ANNEXE 1 — Structure du jeu de données
# MAGIC
# MAGIC Les 23 colonnes de la source, telles qu'ingérées en couche Bronze.

# COMMAND ----------

bronze = spark.table(config.BRONZE_TABLE)

print(f"Table  : {config.BRONZE_TABLE}")
print(f"Lignes : {bronze.count():,}".replace(",", " "))
print(f"Colonnes : {len(bronze.columns)} "
      f"(23 issues de la source + 3 de traçabilité)")
print()
bronze.printSchema()

# COMMAND ----------

# Aperçu des premières lignes. À capturer conjointement au schéma ci-dessus.
display(
    bronze.select(
        "trans_date_trans_time", "cc_num", "merchant", "category", "amt",
        "city", "state", "zip", "lat", "long", "trans_num", "is_fraud",
    ).limit(8)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ANNEXE 2 — Analyse exploratoire
# MAGIC
# MAGIC Quatre figures. Les trois premières justifient des décisions de conception ;
# MAGIC la quatrième justifie le choix des largeurs de fenêtre.

# COMMAND ----------

silver = spark.table(config.SILVER_TABLE)

# --- Figure 1 : taux de fraude par heure ----------------------------------
hourly = (
    silver.groupBy(F.hour("event_time").alias("heure"))
    .agg((100 * F.avg("is_fraud")).alias("taux"))
    .orderBy("heure")
    .toPandas()
)

fig, ax = plt.subplots(figsize=(11, 4.5))
colors = [FRAUD_COLOR if h in (22, 23, 0, 1, 2, 3) else LEGIT_COLOR for h in hourly["heure"]]
ax.bar(hourly["heure"], hourly["taux"], color=colors)
ax.set_xlabel("Heure de la journée")
ax.set_ylabel("Taux de fraude (%)")
ax.set_title("Taux de fraude selon l'heure — la rupture est nette entre 21 h et 22 h", pad=12)
ax.set_xticks(range(24))
ax.axhline(hourly["taux"].median(), color="grey", linestyle="--", linewidth=1,
           label=f"médiane : {hourly['taux'].median():.3f} %")
ax.legend()
plt.tight_layout()
plt.show()

print(f"Plage nocturne (22h-03h) : {hourly[hourly['heure'].isin([22,23,0,1,2,3])]['taux'].min():.3f} % "
      f"à {hourly[hourly['heure'].isin([22,23,0,1,2,3])]['taux'].max():.3f} %")
print(f"Plage diurne   (04h-21h) : {hourly[~hourly['heure'].isin([22,23,0,1,2,3])]['taux'].min():.3f} % "
      f"à {hourly[~hourly['heure'].isin([22,23,0,1,2,3])]['taux'].max():.3f} %")

# COMMAND ----------

# --- Figure 2 : distribution des montants ---------------------------------
# Toutes les fraudes, et un échantillon de transactions légitimes : conserver
# 1,84 M de points n'apporterait rien à la lecture d'un histogramme.
amounts = (
    silver.select("amount", "is_fraud")
    .sampleBy("is_fraud", fractions={0: 0.06, 1: 1.0}, seed=42)
    .toPandas()
)

fig, ax = plt.subplots(figsize=(11, 4.5))
bins = np.logspace(0, np.log10(amounts["amount"].max()), 60)
ax.hist(amounts.loc[amounts["is_fraud"] == 0, "amount"], bins=bins, alpha=0.65,
        color=LEGIT_COLOR, label="Légitimes (échantillon)", density=True)
ax.hist(amounts.loc[amounts["is_fraud"] == 1, "amount"], bins=bins, alpha=0.65,
        color=FRAUD_COLOR, label="Fraudes", density=True)
ax.set_xscale("log")
ax.set_xlabel("Montant ($, échelle logarithmique)")
ax.set_ylabel("Densité")
ax.set_title("Distribution des montants — les fraudes se concentrent sur une plage étroite", pad=12)
ax.legend()
plt.tight_layout()
plt.show()

stats = silver.groupBy("is_fraud").agg(
    F.round(F.avg("amount"), 2).alias("moyenne"),
    F.round(F.expr("percentile_approx(amount, 0.5)"), 2).alias("mediane"),
    F.round(F.max("amount"), 2).alias("maximum"),
).orderBy("is_fraud")
display(stats)

# COMMAND ----------

# --- Figure 3 : distance porteur / commerçant -----------------------------
# Variable préconisée dans la méthodologie initiale, puis écartée : cette
# figure en est la justification.
EARTH_RADIUS_KM = 6371.0

distance_expr = (
    2 * EARTH_RADIUS_KM * F.asin(F.sqrt(
        F.pow(F.sin((F.radians("merchant_lat") - F.radians("customer_lat")) / 2), 2)
        + F.cos(F.radians("customer_lat")) * F.cos(F.radians("merchant_lat"))
        * F.pow(F.sin((F.radians("merchant_lon") - F.radians("customer_lon")) / 2), 2)
    ))
)

distances = (
    silver.withColumn("distance_km", distance_expr)
    .select("distance_km", "is_fraud")
    .sampleBy("is_fraud", fractions={0: 0.06, 1: 1.0}, seed=42)
    .toPandas()
)

fig, ax = plt.subplots(figsize=(11, 4.5))
bins = np.linspace(0, 160, 50)
ax.hist(distances.loc[distances["is_fraud"] == 0, "distance_km"], bins=bins, alpha=0.6,
        color=LEGIT_COLOR, label="Légitimes (échantillon)", density=True)
ax.hist(distances.loc[distances["is_fraud"] == 1, "distance_km"], bins=bins, alpha=0.6,
        color=FRAUD_COLOR, label="Fraudes", density=True)
ax.set_xlabel("Distance porteur – commerçant (km)")
ax.set_ylabel("Densité")
ax.set_title("Distance géographique — distributions indiscernables, variable écartée", pad=12)
ax.legend()
plt.tight_layout()
plt.show()

print(distances.groupby("is_fraud")["distance_km"].agg(["mean", "median"]).round(2).to_string())

# COMMAND ----------

# --- Figure 4 : cadence des transactions ----------------------------------
# Justifie les largeurs de fenêtre retenues en couche Gold.
gold = spark.table(config.GOLD_FEATURES_TABLE)

delays = (
    gold.filter(F.col("seconds_since_previous_txn").isNotNull())
    .select((F.col("seconds_since_previous_txn") / 3600).alias("heures"), "is_fraud")
    .sampleBy("is_fraud", fractions={0: 0.06, 1: 1.0}, seed=42)
    .toPandas()
)

fig, ax = plt.subplots(figsize=(11, 4.5))
bins = np.logspace(-2, 2.5, 60)
ax.hist(delays.loc[delays["is_fraud"] == 0, "heures"], bins=bins, alpha=0.6,
        color=LEGIT_COLOR, label="Légitimes (échantillon)", density=True)
ax.hist(delays.loc[delays["is_fraud"] == 1, "heures"], bins=bins, alpha=0.6,
        color=FRAUD_COLOR, label="Fraudes", density=True)
ax.set_xscale("log")
ax.axvline(1, color="black", linestyle="--", linewidth=1.2, label="fenêtre 1 h")
ax.axvline(24, color="grey", linestyle="--", linewidth=1.2, label="fenêtre 24 h")
ax.set_xlabel("Délai depuis la transaction précédente (heures, échelle log)")
ax.set_ylabel("Densité")
ax.set_title("Cadence des transactions — les fraudes surviennent en rafales", pad=12)
ax.legend()
plt.tight_layout()
plt.show()

print(delays.groupby("is_fraud")["heures"].median().round(2).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ANNEXE 5 — Pseudonymisation : cardinalité avant et après
# MAGIC
# MAGIC La preuve d'absence de collision de hachage suppose de comparer le nombre
# MAGIC de porteurs **avant** et **après** transformation. Le notebook de la couche
# MAGIC Silver ne vérifie que l'état final ; cette cellule affiche les deux.

# COMMAND ----------

avant = spark.table(config.BRONZE_TABLE).select("cc_num").distinct().count()
apres = spark.table(config.SILVER_TABLE).select("customer_id").distinct().count()

print("Cardinalité des porteurs de carte")
print("-" * 52)
print(f"  Avant (cc_num en clair, Bronze)      : {avant}")
print(f"  Après (customer_id haché, Silver)    : {apres}")
print(f"  Écart                                : {apres - avant}")
print()
print("Une cardinalité inférieure révélerait une collision de hachage :")
print("deux porteurs distincts confondus sous un même identifiant, ce qui")
print("mélangerait leurs historiques et fausserait toutes les variables")
print("comportementales. Une cardinalité supérieure signalerait un calcul")
print("non déterministe.")
print()
print("CONFORME" if avant == apres else "ANOMALIE — collision détectée")

# COMMAND ----------

# Illustration : le même numéro produit toujours la même empreinte, et deux
# numéros voisins produisent des empreintes sans rapport.
display(
    spark.table(config.SILVER_TABLE)
    .select("customer_id")
    .distinct()
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ANNEXE 6 — Volumétrie de la couche Gold et valeurs manquantes

# COMMAND ----------

gold_count = gold.count()

print(f"Table                        : {config.GOLD_FEATURES_TABLE}")
print(f"Lignes                       : {gold_count:,}".replace(",", " "))
print(f"Colonnes totales             : {len(gold.columns)}")
print(f"Variables explicatives       : {len(features.FEATURE_COLUMNS)}")
print(f"Variables comportementales créées : {len(features.GOLD_DATA_DICTIONARY) - 1}")
print()
print("Variables explicatives soumises au modèle :")
for i, column in enumerate(features.FEATURE_COLUMNS, 1):
    print(f"  {i:>2}. {column}")

# COMMAND ----------

# Valeurs manquantes : elles doivent correspondre exactement aux premières
# transactions de chaque porteur. Un écart révélerait une erreur de
# partitionnement des fenêtres.
nullable = [
    "seconds_since_previous_txn", "customer_amount_mean_hist",
    "amount_ratio_vs_mean", "customer_category_share_hist",
    "customer_amount_std_hist", "amount_zscore_vs_history",
]
missing = gold.select(
    *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in nullable]
).collect()[0]

n_customers = gold.select("customer_id").distinct().count()

print(f"{'Variable':<32}{'Manquantes':>12}{'Taux':>10}   Interprétation")
print("-" * 84)
for column in nullable:
    n = int(missing[column] or 0)
    ratio = n / n_customers
    lecture = f"{ratio:.0f} par porteur" if ratio == int(ratio) else "à vérifier"
    print(f"{column:<32}{n:>12,}{100 * n / gold_count:>9.4f}%   {lecture}".replace(",", " "))

print()
print(f"Porteurs distincts : {n_customers}")
print("Une valeur par porteur correspond à sa première transaction, sans")
print("antécédent. Deux valeurs correspondent aux deux premières : il faut")
print("au moins deux observations pour calculer un écart-type.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ANNEXE 9 — Courbe du coût métier
# MAGIC
# MAGIC La figure centrale de la Partie 3 : elle montre les deux coûts qui
# MAGIC s'opposent, leur somme, et le seuil qui la minimise.

# COMMAND ----------

model = mlflow.sklearn.load_model(
    f"models:/{config.REGISTERED_MODEL_NAME}@{config.MODEL_ALIAS}"
)
datasets = modeling.split_datasets(modeling.to_pandas(modeling.add_temporal_split(gold)))
validation = datasets["validation"]
validation_scores = model.predict_proba(validation.X)[:, 1]

curve = metrics.cost_curve(
    validation.y, validation_scores, validation.X["amount"],
    false_alert_cost=config.FALSE_ALERT_COST,
)
optimum = metrics.optimal_threshold_by_cost(curve)
baseline = curve.iloc[0]

print(f"Seuil optimal        : {optimum['threshold']:.4f}")
print(f"Coût sans dispositif : {baseline['total_cost']:>12,.0f} $".replace(",", " "))
print(f"Coût au seuil optimal: {optimum['total_cost']:>12,.0f} $".replace(",", " "))
print(f"Économie             : {baseline['total_cost'] - optimum['total_cost']:>12,.0f} $ "
      f"({100 * (baseline['total_cost'] - optimum['total_cost']) / baseline['total_cost']:.1f} %)".replace(",", " "))

# COMMAND ----------

# La courbe est tracée sur une plage de seuils lisible : au-delà de 0,5, les
# trois coûts sont plats et n'apportent rien à la lecture.
plot_data = curve[(curve["threshold"] > 0) & (curve["threshold"] <= 0.6)].copy()

fig, ax = plt.subplots(figsize=(11, 5.5))
ax.plot(plot_data["threshold"], plot_data["fraud_amount_missed"],
        color=FRAUD_COLOR, label="Fraude subie (non détectée)", linewidth=2)
ax.plot(plot_data["threshold"], plot_data["alert_cost"],
        color=LEGIT_COLOR, label="Coût des fausses alertes", linewidth=2)
ax.plot(plot_data["threshold"], plot_data["total_cost"],
        color="black", label="Coût total", linewidth=2.5)

ax.axvline(optimum["threshold"], color="green", linestyle="--", linewidth=1.5)
ax.annotate(
    f"Optimum : seuil {optimum['threshold']:.4f}\nCoût total {optimum['total_cost']:,.0f} $".replace(",", " "),
    xy=(optimum["threshold"], optimum["total_cost"]),
    xytext=(optimum["threshold"] + 0.08, optimum["total_cost"] + plot_data["total_cost"].max() * 0.25),
    arrowprops=dict(arrowstyle="->", color="green"),
    fontsize=10,
)
ax.axvline(0.5, color="grey", linestyle=":", linewidth=1.2)
ax.text(0.5, plot_data["total_cost"].max() * 0.9, " seuil par défaut (0,5)",
        fontsize=9, color="grey")

ax.set_xlabel("Seuil de décision")
ax.set_ylabel("Coût ($)")
ax.set_title("Coût métier en fonction du seuil — bloc de validation", pad=12)
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ANNEXE 10 — Importance des variables (SHAP)
# MAGIC
# MAGIC Les contributions sont **regroupées par variable d'origine** : sans cela,
# MAGIC `customer_state` paraîtrait négligeable, son influence étant répartie sur
# MAGIC cinquante et une colonnes binaires issues de l'encodage.

# COMMAND ----------

preprocessor = model.named_steps["preprocessing"]
classifier = model.named_steps["classifier"]

sample = validation.X.sample(n=min(20_000, len(validation.X)), random_state=42)
sample_transformed = preprocessor.transform(sample)
transformed_names = list(preprocessor.get_feature_names_out())

shap_values = shap.TreeExplainer(classifier).shap_values(sample_transformed)


def original_feature(name: str) -> str:
    name = name.split("__", 1)[-1]
    if name.startswith("missingindicator_"):
        return f"{name.removeprefix('missingindicator_')} (manquant)"
    for categorical in modeling.CATEGORICAL_FEATURES:
        if name.startswith(f"{categorical}_"):
            return categorical
    return name


importance = (
    pd.DataFrame({
        "feature": [original_feature(n) for n in transformed_names],
        "importance": np.abs(shap_values).mean(axis=0),
    })
    .groupby("feature", as_index=False)["importance"].sum()
    .sort_values("importance", ascending=False, ignore_index=True)
)
importance["part_pct"] = 100 * importance["importance"] / importance["importance"].sum()

top = importance.head(15).iloc[::-1]

fig, ax = plt.subplots(figsize=(10, 7))
ax.barh(top["feature"], top["part_pct"], color=LEGIT_COLOR)
for y, (value, name) in enumerate(zip(top["part_pct"], top["feature"])):
    ax.text(value + 0.3, y, f"{value:.2f} %", va="center", fontsize=9)
ax.set_xlabel("Contribution moyenne au score (%)")
ax.set_title("Importance des variables — valeurs de Shapley regroupées par variable d'origine", pad=12)
ax.set_xlim(0, top["part_pct"].max() * 1.18)
plt.tight_layout()
plt.show()

print(importance.head(15).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC Figures produites, prêtes à capturer :
# MAGIC
# MAGIC | Section | Annexe | Figure |
# MAGIC |---|---|---|
# MAGIC | 1 | A1 | Schéma des 23 colonnes + aperçu |
# MAGIC | 2 | A2 | Taux de fraude horaire · montants · distance · cadence |
# MAGIC | 3 | A5 | Cardinalité avant / après pseudonymisation |
# MAGIC | 4 | A6 | Volumétrie Gold, 26 variables, valeurs manquantes |
# MAGIC | 5 | A9 | Courbe du coût métier avec l'optimum |
# MAGIC | 6 | A10 | Importance des variables |
# MAGIC
# MAGIC Les autres annexes se capturent directement dans les notebooks du pipeline
# MAGIC ou dans l'interface Databricks.
