# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Seuil de décision, explicabilité et évaluation finale
# MAGIC
# MAGIC **C'est ici que se joue la problématique du mémoire.**
# MAGIC
# MAGIC Jusqu'à présent, toutes les métriques ont été calculées au seuil de 0,5 —
# MAGIC une valeur arbitraire, héritée du fait qu'un classifieur binaire renvoie une
# MAGIC probabilité. Or ce n'est **pas le modèle qui produit les faux positifs, c'est
# MAGIC le seuil**. Le même modèle, selon l'endroit où l'on coupe, peut bloquer une
# MAGIC transaction légitime sur mille ou une sur dix.
# MAGIC
# MAGIC ## Pourquoi ne pas maximiser le F1
# MAGIC
# MAGIC Le réflexe serait de retenir le seuil qui maximise le F1. Ce serait poser,
# MAGIC sans le dire, qu'**un faux positif et un faux négatif se valent**.
# MAGIC
# MAGIC C'est faux dans tous les métiers, et particulièrement en fraude :
# MAGIC
# MAGIC | Erreur | Nature du coût | Mesurable ? |
# MAGIC |---|---|---|
# MAGIC | **Faux négatif** — fraude non détectée | Le montant de la transaction | **Oui**, il est dans les données |
# MAGIC | **Faux positif** — transaction légitime bloquée | Temps d'analyste, contact client, friction commerciale | **Non**, il faut le poser en hypothèse |
# MAGIC
# MAGIC Le seuil retenu est donc celui qui **minimise le coût métier total**. Comme
# MAGIC il dépend de l'hypothèse de coût d'une fausse alerte, une analyse de
# MAGIC sensibilité est conduite sur plusieurs valeurs plutôt que de faire reposer la
# MAGIC conclusion sur un chiffre unique.
# MAGIC
# MAGIC ## Le seuil est choisi sur la validation, jamais sur le test
# MAGIC
# MAGIC Le seuil est un **paramètre**. L'optimiser sur le jeu de test rendrait
# MAGIC optimiste le taux de faux positifs annoncé — c'est-à-dire le chiffre central
# MAGIC du mémoire. Le bloc de test n'est utilisé qu'à la toute fin, une seule fois.

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

import mlflow
import numpy as np
import pandas as pd
import shap
from pyspark.sql import functions as F

from fraud import config, metrics, modeling

mlflow.set_registry_uri("databricks-uc")

print(config.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Chargement du modèle depuis le registre
# MAGIC
# MAGIC Le modèle est chargé par son **alias** `@champion`, non par un numéro de
# MAGIC version. Promouvoir un nouveau modèle se réduit ainsi à déplacer l'alias,
# MAGIC sans modifier une ligne de ce notebook — c'est la séparation entre le cycle
# MAGIC de vie du modèle et celui du code qui le consomme.

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()


def resolve_champion_version() -> str:
    """Retourne la version portant l'alias, en la posant si elle manque.

    L'alias peut ne pas exister — notamment si le notebook d'entraînement a été
    exécuté avant son introduction. Plutôt que d'échouer, on retombe sur la
    version la plus récente du registre et on pose l'alias dessus, ce qui rend
    ce notebook exécutable seul.
    """
    try:
        version = client.get_model_version_by_alias(
            config.REGISTERED_MODEL_NAME, config.MODEL_ALIAS
        )
        print(f"Alias '{config.MODEL_ALIAS}' -> version {version.version}")
        return version.version
    except Exception:
        print(f"Alias '{config.MODEL_ALIAS}' absent : reprise sur la dernière version.")

    versions = client.search_model_versions(f"name = '{config.REGISTERED_MODEL_NAME}'")
    if not versions:
        raise RuntimeError(
            f"Aucune version enregistrée pour {config.REGISTERED_MODEL_NAME}. "
            "Exécuter d'abord le notebook 04_model_training."
        )

    latest = max(versions, key=lambda v: int(v.version))
    client.set_registered_model_alias(
        config.REGISTERED_MODEL_NAME, config.MODEL_ALIAS, latest.version
    )
    print(f"Alias '{config.MODEL_ALIAS}' posé sur la version {latest.version}.")
    return latest.version


champion_version = resolve_champion_version()

# --- Vérification : est-ce bien le modèle retenu à l'étape 4 ? -------------
# Le registre contient aussi les candidats écartés. Scorer avec l'un d'eux
# passerait totalement inaperçu : le notebook s'exécuterait, produirait des
# métriques plausibles, et le mémoire annoncerait les performances d'un modèle
# qui n'est pas celui qu'il décrit.
version_info = client.get_model_version(config.REGISTERED_MODEL_NAME, champion_version)
registered_algorithm = version_info.tags.get("algorithm")

print(f"Configuration enregistrée : {registered_algorithm or 'non renseignée'}")
print(f"Configuration attendue    : {config.FINAL_MODEL_NAME}")

if registered_algorithm and registered_algorithm != config.FINAL_MODEL_NAME:
    raise ValueError(
        f"La version {champion_version} correspond à '{registered_algorithm}', "
        f"alors que le modèle retenu est '{config.FINAL_MODEL_NAME}'.\n"
        "Réexécuter la section 5 du notebook 04_model_training pour enregistrer "
        "et aliaser la bonne configuration."
    )
if not registered_algorithm:
    print("AVERTISSEMENT : version sans étiquette 'algorithm', vérification impossible.")

# COMMAND ----------

model_uri = f"models:/{config.REGISTERED_MODEL_NAME}@{config.MODEL_ALIAS}"
model = mlflow.sklearn.load_model(model_uri)

print(f"Modèle chargé : {model_uri}  (version {champion_version})")
print(f"Étapes du pipeline : {list(model.named_steps)}")

# COMMAND ----------

gold = modeling.add_temporal_split(spark.table(config.GOLD_FEATURES_TABLE))
pdf = modeling.to_pandas(gold)
datasets = modeling.split_datasets(pdf)

validation, test = datasets["validation"], datasets["test"]

for dataset in (validation, test):
    print(dataset.describe())

# COMMAND ----------

validation_scores = model.predict_proba(validation.X)[:, 1]
print(f"Scores calculés sur {len(validation_scores):,} transactions de validation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Le coût métier en fonction du seuil
# MAGIC
# MAGIC Pour chaque seuil candidat, deux coûts s'opposent :
# MAGIC
# MAGIC - **le montant des fraudes non détectées** — il décroît quand le seuil
# MAGIC   s'abaisse, puisqu'on attrape davantage ;
# MAGIC - **le coût des fausses alertes** — il croît quand le seuil s'abaisse,
# MAGIC   puisqu'on dérange davantage de clients honnêtes.
# MAGIC
# MAGIC Leur somme passe par un minimum : c'est le seuil optimal.
# MAGIC
# MAGIC La première ligne de la courbe correspond à **l'absence d'alerte** — la
# MAGIC référence « aucun modèle », où toute la fraude est subie. Sans elle, on ne
# MAGIC saurait pas si le dispositif fait gagner ou perdre de l'argent.

# COMMAND ----------

curve = metrics.cost_curve(
    validation.y,
    validation_scores,
    validation.X["amount"],
    false_alert_cost=config.FALSE_ALERT_COST,
)

baseline = curve.iloc[0]
optimum = metrics.optimal_threshold_by_cost(curve)

print(f"Hypothèse de coût d'une fausse alerte : {config.FALSE_ALERT_COST:.2f} $")
print()
print("SANS MODÈLE (aucune alerte)")
print(f"  Fraude subie          : {baseline['fraud_amount_missed']:>12,.0f} $".replace(",", " "))
print()
print("AU SEUIL OPTIMAL")
print(f"  Seuil                 : {optimum['threshold']:>12.4f}")
print(f"  Alertes émises        : {optimum['alerts']:>12,.0f}".replace(",", " "))
print(f"  Fraudes détectées     : {optimum['true_positives']:>12,.0f}  ({100 * optimum['recall']:.1f} %)".replace(",", " "))
print(f"  Fausses alertes       : {optimum['false_positives']:>12,.0f}".replace(",", " "))
print(f"  Fraude subie          : {optimum['fraud_amount_missed']:>12,.0f} $".replace(",", " "))
print(f"  Coût des alertes      : {optimum['alert_cost']:>12,.0f} $".replace(",", " "))
print(f"  COÛT TOTAL            : {optimum['total_cost']:>12,.0f} $".replace(",", " "))
print()
gain = baseline["total_cost"] - optimum["total_cost"]
print(f"Économie par rapport à l'absence de dispositif : {gain:,.0f} $ "
      f"({100 * gain / baseline['total_cost']:.1f} %)".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Analyse de sensibilité au coût d'une fausse alerte
# MAGIC
# MAGIC Le coût d'un faux positif est une **hypothèse**, pas une mesure. Présenter
# MAGIC un seuil unique reviendrait à masquer que la conclusion en dépend.
# MAGIC
# MAGIC Le tableau ci-dessous montre comment le point de fonctionnement se déplace
# MAGIC selon la valeur retenue. C'est cette dépendance qui doit être exposée au
# MAGIC métier : ce n'est pas au data scientist de décider ce que coûte un client
# MAGIC dérangé à tort.

# COMMAND ----------

print(f"{'Coût FP':>9}{'Seuil':>10}{'Rappel':>9}{'Précision':>11}"
      f"{'Fausses alertes':>17}{'TFP':>9}{'Coût total':>14}")
print("-" * 79)

sensitivity = []
n_legit = int((validation.y == 0).sum())
for cost in config.FALSE_ALERT_COST_SCENARIOS:
    point = metrics.optimal_threshold_by_cost(
        metrics.cost_curve(validation.y, validation_scores, validation.X["amount"], cost)
    )
    fpr = point["false_positives"] / n_legit
    sensitivity.append({"cost": cost, "threshold": point["threshold"], "fpr": fpr})
    print(
        f"{cost:>8.0f}${point['threshold']:>10.4f}{100 * point['recall']:>8.1f}%"
        f"{100 * point['precision']:>10.1f}%{point['false_positives']:>17,.0f}"
        f"{fpr:>9.5f}{point['total_cost']:>13,.0f}$".replace(",", " ")
    )

print()
print("Lecture : plus une fausse alerte coûte cher, plus le seuil devient")
print("sélectif — et plus le taux de faux positifs baisse, au prix du rappel.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Comparaison des stratégies de seuil
# MAGIC
# MAGIC Quatre façons de choisir le seuil, évaluées sur les mêmes prédictions.

# COMMAND ----------

sweep = metrics.threshold_sweep(validation.y, validation_scores)
f1_optimal = sweep.loc[sweep["f1"].idxmax(), "threshold"]

candidates = {
    "Défaut (0,5)": 0.5,
    "F1 maximal": float(f1_optimal),
    "Coût métier minimal": float(optimum["threshold"]),
    "Rappel ≥ 90 %": float(metrics.best_threshold_for_recall(sweep, 0.90)["threshold"]),
}

print(f"{'Stratégie':<24}{'Seuil':>9}{'Précision':>11}{'Rappel':>9}"
      f"{'Faux positifs':>15}{'TFP':>9}")
print("-" * 77)
for label, threshold in candidates.items():
    m = metrics.evaluate(validation.y, validation_scores, threshold=threshold)
    print(
        f"{label:<24}{threshold:>9.4f}{m['precision']:>11.4f}{m['recall']:>9.4f}"
        f"{m['false_positives']:>15,}{m['false_positive_rate']:>9.5f}".replace(",", " ")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Le seuil retenu

# COMMAND ----------

DECISION_THRESHOLD = float(optimum["threshold"])
print(f"Seuil de décision retenu : {DECISION_THRESHOLD:.6f}")
print(f"  choisi sur le bloc de validation, par minimisation du coût métier")
print(f"  hypothèse : une fausse alerte coûte {config.FALSE_ALERT_COST:.2f} $")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Explicabilité — analyse SHAP
# MAGIC
# MAGIC En contexte bancaire réglementé, un refus de transaction doit pouvoir être
# MAGIC justifié. Un modèle qui ne sait pas expliquer ses décisions n'est pas
# MAGIC déployable, quelle que soit sa performance.
# MAGIC
# MAGIC SHAP attribue à chaque variable sa contribution à l'écart entre la
# MAGIC prédiction et la prédiction moyenne. Contrairement à l'importance native de
# MAGIC XGBoost — qui compte les usages d'une variable dans les arbres —, elle est
# MAGIC **additive et locale** : elle explique chaque prédiction individuellement.
# MAGIC
# MAGIC ### Deux vérifications ciblées
# MAGIC
# MAGIC L'analyse répond à deux questions ouvertes depuis l'étape 3 :
# MAGIC
# MAGIC 1. **L'heure écrase-t-elle les autres variables ?** Le profilage a montré un
# MAGIC    taux de fraude 10 à 30 fois supérieur la nuit, en partie par artefact du
# MAGIC    générateur. Si l'heure domine, le modèle apprend la règle de génération
# MAGIC    plutôt qu'un comportement de fraude.
# MAGIC 2. **`customer_txn_count_hist` mesure-t-elle un comportement ou une date ?**
# MAGIC    Cette variable croît mécaniquement avec le temps écoulé, or le taux de
# MAGIC    fraude décroît sur la période. Elle pourrait n'être qu'un indicateur
# MAGIC    temporel déguisé, qui ne généraliserait pas.

# COMMAND ----------

SHAP_SAMPLE_SIZE = 20_000

preprocessor = model.named_steps["preprocessing"]
classifier = model.named_steps["classifier"]

sample = validation.X.sample(n=min(SHAP_SAMPLE_SIZE, len(validation.X)), random_state=42)
sample_transformed = preprocessor.transform(sample)
transformed_names = list(preprocessor.get_feature_names_out())

explainer = shap.TreeExplainer(classifier)
shap_values = explainer.shap_values(sample_transformed)

print(f"SHAP calculé sur {len(sample):,} transactions et "
      f"{len(transformed_names)} colonnes transformées.".replace(",", " "))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Regroupement par variable d'origine
# MAGIC
# MAGIC Le prétraitement éclate `customer_state` en 51 colonnes binaires. Comparer
# MAGIC ces colonnes une à une aux variables numériques fausserait le classement :
# MAGIC une variable catégorielle apparaîtrait artificiellement faible parce que son
# MAGIC influence est répartie sur cinquante et une contributions.
# MAGIC
# MAGIC Les contributions sont donc **sommées par variable d'origine**, ce que
# MAGIC l'additivité de SHAP autorise.

# COMMAND ----------

def original_feature(transformed_name: str) -> str:
    """Retrouve la variable d'origine d'une colonne issue du prétraitement."""
    name = transformed_name.split("__", 1)[-1]

    if name.startswith("missingindicator_"):
        return f"{name.removeprefix('missingindicator_')} (indicateur manquant)"

    # Colonnes issues de l'encodage one-hot : `<variable>_<modalité>`.
    for categorical in modeling.CATEGORICAL_FEATURES:
        if name.startswith(f"{categorical}_"):
            return categorical
    return name


importance = (
    pd.DataFrame(
        {
            "feature": [original_feature(n) for n in transformed_names],
            "importance": np.abs(shap_values).mean(axis=0),
        }
    )
    .groupby("feature", as_index=False)["importance"]
    .sum()
    .sort_values("importance", ascending=False, ignore_index=True)
)
importance["part_pct"] = 100 * importance["importance"] / importance["importance"].sum()
importance["cumul_pct"] = importance["part_pct"].cumsum()

print(f"{'#':>3}  {'Variable':<42}{'Part':>8}{'Cumul':>9}")
print("-" * 64)
for i, row in importance.head(20).iterrows():
    print(f"{i + 1:>3}  {row['feature']:<42}{row['part_pct']:>7.2f}%{row['cumul_pct']:>8.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Réponses aux deux vérifications

# COMMAND ----------

def share_of(*features: str) -> float:
    return float(importance[importance["feature"].isin(features)]["part_pct"].sum())


def rank_of(feature: str) -> str:
    match = importance.index[importance["feature"] == feature]
    return f"{int(match[0]) + 1}" if len(match) else "absente"


temporal = share_of("hour_sin", "hour_cos", "is_night", "hour_of_day", "day_of_week", "is_weekend")
behavioural = share_of(
    "amount_zscore_vs_history", "amount_ratio_vs_mean", "customer_amount_mean_hist",
    "customer_amount_std_hist", "customer_amount_max_hist",
)
velocity = share_of(
    "txn_count_1h", "txn_count_24h", "txn_count_7d",
    "amount_sum_1h", "amount_sum_24h", "amount_sum_7d", "seconds_since_previous_txn",
)

print("Poids par famille de variables")
print(f"  Temporelles (heure, jour)         : {temporal:>6.2f} %")
print(f"  Écart au comportement habituel    : {behavioural:>6.2f} %")
print(f"  Vélocité                          : {velocity:>6.2f} %")
print()
print("Vérification 1 — l'heure domine-t-elle ?")
print(f"  Part des variables temporelles : {temporal:.2f} %")
print(f"  {'OUI — le modèle apprend surtout la règle horaire du générateur.' if temporal > 50 else 'NON — le signal horaire est présent sans écraser les autres.'}")
print()
print("Vérification 2 — customer_txn_count_hist est-elle un indicateur temporel ?")
print(f"  Rang : {rank_of('customer_txn_count_hist')} sur {len(importance)}")
print(f"  Part : {share_of('customer_txn_count_hist'):.2f} %")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explication individuelle d'une alerte
# MAGIC
# MAGIC C'est la forme sous laquelle un analyste recevrait la justification d'un
# MAGIC blocage : les variables qui ont poussé *cette* transaction vers la fraude.

# COMMAND ----------

sample_scores = model.predict_proba(sample)[:, 1]
top_alert = int(np.argmax(sample_scores))

contributions = (
    pd.DataFrame(
        {
            "feature": [original_feature(n) for n in transformed_names],
            "contribution": shap_values[top_alert],
        }
    )
    .groupby("feature", as_index=False)["contribution"]
    .sum()
)
contributions["abs"] = contributions["contribution"].abs()
contributions = contributions.sort_values("abs", ascending=False, ignore_index=True)

print(f"Transaction la plus suspecte de l'échantillon — score {sample_scores[top_alert]:.4f}")
print()
print(f"{'Variable':<40}{'Valeur':>14}{'Contribution':>14}   Sens")
print("-" * 82)
for _, row in contributions.head(8).iterrows():
    raw = sample.iloc[top_alert].get(row["feature"], "—")
    shown = f"{raw:.3f}" if isinstance(raw, (int, float, np.number)) else str(raw)[:14]
    direction = "vers FRAUDE" if row["contribution"] > 0 else "vers légitime"
    print(f"{row['feature']:<40}{shown:>14}{row['contribution']:>14.4f}   {direction}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Évaluation finale sur le bloc de test
# MAGIC
# MAGIC **Le bloc de test est utilisé ici pour la première et unique fois.** Le
# MAGIC modèle et le seuil ont été fixés sur l'entraînement et la validation, sans
# MAGIC jamais le consulter.
# MAGIC
# MAGIC L'écart entre validation et test mesure la capacité du dispositif à tenir sur
# MAGIC une période qu'il n'a jamais vue — ce qui est exactement la question posée en
# MAGIC production.

# COMMAND ----------

test_scores = model.predict_proba(test.X)[:, 1]

validation_metrics = metrics.evaluate(validation.y, validation_scores, DECISION_THRESHOLD)
test_metrics = metrics.evaluate(test.y, test_scores, DECISION_THRESHOLD)

print(f"{'Métrique':<26}{'Validation':>13}{'Test':>13}{'Écart':>11}")
print("-" * 63)
for key, label in [
    ("auc_pr", "AUC-PR"),
    ("auc_roc", "AUC-ROC"),
    ("precision", "Précision"),
    ("recall", "Rappel"),
    ("f1", "F1"),
    ("false_positive_rate", "Taux de faux positifs"),
    ("alerts_per_1000", "Alertes / 1000 txn"),
]:
    a, b = validation_metrics[key], test_metrics[key]
    print(f"{label:<26}{a:>13.5f}{b:>13.5f}{b - a:>+11.5f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Matrice de confusion sur le test

# COMMAND ----------

m = test_metrics
print("                     Prédit légitime   Prédit fraude")
print(f"Réellement légitime  {m['true_negatives']:>15,}   {m['false_positives']:>13,}".replace(",", " "))
print(f"Réellement fraude    {m['false_negatives']:>15,}   {m['true_positives']:>13,}".replace(",", " "))
print()
print(f"Fraudes détectées      : {m['true_positives']:,} sur {m['true_positives'] + m['false_negatives']:,}".replace(",", " "))
print(f"Fausses alertes        : {m['false_positives']:,}".replace(",", " "))
print(f"Alertes à traiter      : {m['true_positives'] + m['false_positives']:,}".replace(",", " "))
print(f"Précision des alertes  : {100 * m['precision']:.1f} % — soit environ "
      f"{(m['true_positives'] + m['false_positives']) / max(m['true_positives'], 1):.1f} alertes examinées par fraude trouvée")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Table scorée
# MAGIC
# MAGIC L'ensemble du corpus est scoré et écrit en couche Gold. Cette table alimente
# MAGIC le tableau de bord de l'étape 7.
# MAGIC
# MAGIC Le seuil de décision est enregistré comme **propriété de la table** : la
# MAGIC décision `is_flagged` n'est interprétable que si l'on sait à quel seuil elle
# MAGIC a été prise, et cette information doit voyager avec les données.

# COMMAND ----------

all_scores = model.predict_proba(pdf[list(modeling.ALL_FEATURES)])[:, 1]

scored_pdf = pd.DataFrame(
    {
        "transaction_id": pdf["transaction_id"].values,
        "fraud_score": all_scores.astype("float64"),
    }
)

scored = (
    modeling.add_temporal_split(spark.table(config.GOLD_FEATURES_TABLE))
    .join(spark.createDataFrame(scored_pdf), on="transaction_id", how="inner")
    .withColumn("is_flagged", F.col("fraud_score") >= F.lit(DECISION_THRESHOLD))
    .withColumn("decision_threshold", F.lit(DECISION_THRESHOLD))
    .withColumn("_scored_at", F.current_timestamp())
    .select(
        "transaction_id", "customer_id", "event_time", "event_date",
        "amount", "merchant_name", "merchant_category",
        "amount_zscore_vs_history", "txn_count_1h", "is_night",
        "is_fraud", "fraud_score", "is_flagged", "decision_threshold",
        # Le bloc d'appartenance est conservé, et ce n'est pas un détail : sans
        # lui, le tableau de bord agrégerait les trois périodes. Or le modèle a
        # vu les transactions d'entraînement — les métriques calculées dessus
        # sont optimistes par construction. Seul le bloc de test mesure une
        # performance honnête, et le dashboard doit pouvoir s'y restreindre.
        modeling.SPLIT_COL,
        "_scored_at",
    )
)

(
    scored.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(config.GOLD_SCORED_TABLE)
)

spark.sql(f"""
    ALTER TABLE {config.GOLD_SCORED_TABLE} SET TBLPROPERTIES (
        'fraud.decision_threshold' = '{DECISION_THRESHOLD}',
        'fraud.false_alert_cost'   = '{config.FALSE_ALERT_COST}',
        'fraud.model_uri'          = '{model_uri}'
    )
""")

scored_count = spark.table(config.GOLD_SCORED_TABLE).count()
print(f"Table écrite : {config.GOLD_SCORED_TABLE}  ({scored_count:,} lignes)".replace(",", " "))

if scored_count != len(pdf):
    raise ValueError(f"Perte de lignes au scoring : {len(pdf):,} en entrée, {scored_count:,} en sortie.")

# COMMAND ----------

display(
    spark.table(config.GOLD_SCORED_TABLE)
    .filter(F.col("is_flagged"))
    .orderBy(F.col("fraud_score").desc())
    .select("event_time", "amount", "merchant_category", "amount_zscore_vs_history",
            "txn_count_1h", "is_night", "fraud_score", "is_fraud", "split")
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pourquoi les métriques diffèrent selon la période
# MAGIC
# MAGIC Le tableau ci-dessous justifie la présence de la colonne `split`. Le modèle
# MAGIC a **vu** les transactions d'entraînement : les métriques calculées dessus
# MAGIC sont optimistes par construction et ne mesurent rien d'autre que sa capacité
# MAGIC à mémoriser.
# MAGIC
# MAGIC Un tableau de bord agrégeant les trois périodes afficherait donc un taux de
# MAGIC faux positifs flatteur et faux. C'est une erreur d'autant plus insidieuse
# MAGIC qu'elle ne produit aucun message d'erreur.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            split,
            COUNT(*)                                            AS transactions,
            SUM(is_fraud)                                       AS fraudes,
            SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END) AS faux_positifs,
            ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 1 THEN 1 ELSE 0 END)
                  / NULLIF(SUM(is_fraud), 0), 2)                AS rappel_pct,
            ROUND(100.0 * SUM(CASE WHEN is_flagged AND is_fraud = 0 THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END), 0), 4) AS taux_faux_positifs_pct
        FROM {config.GOLD_SCORED_TABLE}
        GROUP BY split
        ORDER BY MIN(event_time)
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Seuil retenu | par minimisation du coût métier, sur la validation |
# MAGIC | Sensibilité | 4 hypothèses de coût d'une fausse alerte |
# MAGIC | Explicabilité | SHAP, contributions regroupées par variable d'origine |
# MAGIC | Évaluation finale | bloc de test, consulté une seule fois |
# MAGIC | Table produite | `gold_scored_transactions` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Points à savoir expliquer en soutenance
# MAGIC
# MAGIC 1. **Pourquoi pas le F1 ?** Le maximiser postule qu'un faux positif et un
# MAGIC    faux négatif se valent. En fraude, le premier coûte du temps d'analyste
# MAGIC    et de la friction client, le second coûte le montant de la transaction —
# MAGIC    montant que les données contiennent, donc qu'il n'y a pas à supposer.
# MAGIC 2. **Pourquoi une analyse de sensibilité ?** Parce que le coût d'une fausse
# MAGIC    alerte est une hypothèse. Annoncer un seuil unique masquerait que la
# MAGIC    conclusion en dépend. Ce n'est pas au data scientist de décider ce que
# MAGIC    coûte un client dérangé à tort — c'est au métier.
# MAGIC 3. **Pourquoi le seuil sur la validation et non sur le test ?** Le seuil est
# MAGIC    un paramètre. L'ajuster sur le test rendrait optimiste le taux de faux
# MAGIC    positifs annoncé, qui est le chiffre central du mémoire.
# MAGIC 4. **Pourquoi SHAP plutôt que l'importance native de XGBoost ?** Cette
# MAGIC    dernière compte les usages d'une variable dans les arbres, ce qui favorise
# MAGIC    les variables à forte cardinalité et n'explique aucune décision
# MAGIC    particulière. SHAP est additive et locale : elle justifie chaque blocage.
# MAGIC 5. **Pourquoi sommer les contributions des colonnes one-hot ?** Sans cela,
# MAGIC    `customer_state` paraîtrait négligeable parce que son influence est
# MAGIC    répartie sur cinquante et une colonnes binaires.
# MAGIC
# MAGIC **Étape suivante** : `06` — orchestration du pipeline en Databricks Workflow.
