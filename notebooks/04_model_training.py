# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Entraînement et comparaison des modèles
# MAGIC
# MAGIC **Objectif** : entraîner plusieurs modèles sur un protocole d'évaluation
# MAGIC honnête, les comparer sur les mêmes données, et justifier celui retenu.
# MAGIC
# MAGIC ## Le protocole compte plus que l'algorithme
# MAGIC
# MAGIC Un XGBoost évalué sur un découpage aléatoire produit des chiffres plus
# MAGIC flatteurs — et beaucoup moins fiables — qu'une régression logistique évaluée
# MAGIC correctement. Trois décisions structurent donc cette étape avant même qu'il
# MAGIC soit question de modèle.
# MAGIC
# MAGIC ### 1. Découpage chronologique à **trois** blocs
# MAGIC
# MAGIC | Bloc | Période | Rôle |
# MAGIC |---|---|---|
# MAGIC | **Entraînement** | 01/2019 → 31/03/2020 | Ajustement des modèles |
# MAGIC | **Validation** | 01/04/2020 → 21/06/2020 | Comparaison des modèles et choix du seuil |
# MAGIC | **Test** | 21/06/2020 → 31/12/2020 | Évaluation finale, **jamais consultée avant la fin** |
# MAGIC
# MAGIC Le bloc de validation n'est pas un raffinement académique. Choisir le seuil
# MAGIC de décision directement sur le test reviendrait à l'optimiser sur les données
# MAGIC censées mesurer la performance : le taux de faux positifs annoncé serait
# MAGIC alors optimiste, et c'est précisément le chiffre central du mémoire.
# MAGIC
# MAGIC ### 2. SMOTE dans le pipeline, jamais à la main
# MAGIC
# MAGIC SMOTE est intégré à un pipeline `imblearn`, ce qui garantit
# MAGIC **structurellement** qu'il ne s'applique qu'à l'entraînement. Appliqué à la
# MAGIC main, il finit tôt ou tard par contaminer la validation — c'est l'erreur la
# MAGIC plus fréquente sur les jeux déséquilibrés, et elle est invisible dans les
# MAGIC métriques.
# MAGIC
# MAGIC ### 3. Le rééquilibrage n'est pas un réglage neutre
# MAGIC
# MAGIC Le réflexe est de rééquilibrer à 50/50. C'est **contraire à la problématique**.
# MAGIC Générer autant de fraudes synthétiques que de transactions légitimes apprend
# MAGIC au modèle que la fraude est un événement sur deux ; il devient alors très
# MAGIC sensible — et produit une avalanche de faux positifs.
# MAGIC
# MAGIC Plusieurs taux de rééquilibrage sont donc comparés, ainsi que l'alternative
# MAGIC `scale_pos_weight` de XGBoost, qui pondère la classe minoritaire sans créer
# MAGIC d'exemples synthétiques.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
import time

REPO_ROOT = os.path.dirname(os.getcwd())
SRC_PATH = os.path.join(REPO_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

import mlflow
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline
from lightgbm import LGBMClassifier
from mlflow.models import infer_signature
from pyspark.sql import functions as F
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier

from fraud import config, modeling

RANDOM_STATE = 42

# L'enregistrement automatique de MLflow capturerait chaque appel scikit-learn,
# y compris les étapes internes du pipeline. On journalise explicitement ce qui
# a un sens pour le mémoire plutôt que tout ce qui est techniquement possible.
mlflow.autolog(disable=True)

current_user = spark.sql("SELECT current_user()").collect()[0][0]
EXPERIMENT_PATH = f"/Users/{current_user}/fraud_detection"
mlflow.set_experiment(EXPERIMENT_PATH)
mlflow.set_registry_uri("databricks-uc")

print(f"Expérience MLflow : {EXPERIMENT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Découpage chronologique

# COMMAND ----------

gold = modeling.add_temporal_split(spark.table(config.GOLD_FEATURES_TABLE))

display(
    gold.groupBy("split")
    .agg(
        F.count("*").alias("transactions"),
        F.sum("is_fraud").alias("fraudes"),
        F.round(100 * F.avg("is_fraud"), 4).alias("taux_fraude_pct"),
        F.min("event_time").alias("debut"),
        F.max("event_time").alias("fin"),
    )
    .orderBy("debut")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Rapatriement en mémoire. Le volume — 1,85 M lignes pour une trentaine de
# MAGIC colonnes — tient confortablement sur un nœud unique après conversion en
# MAGIC simple précision. L'entraînement distribué n'apporterait ici aucun gain et
# MAGIC ajouterait une complexité que rien ne justifie ; le seuil au-delà duquel il
# MAGIC deviendrait nécessaire se situe plutôt vers la dizaine de millions de lignes.

# COMMAND ----------

pdf = modeling.to_pandas(gold)
datasets = modeling.split_datasets(pdf)

print(f"{'Bloc':<12}{'Lignes':>12}{'Fraudes':>10}{'Taux':>10}")
print("-" * 44)
for dataset in datasets.values():
    print(dataset.describe())

print()
print(f"Variables explicatives : {len(modeling.ALL_FEATURES)}")
print(f"Empreinte mémoire      : {pdf.memory_usage(deep=True).sum() / 1024**2:.0f} Mo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vérification du protocole
# MAGIC
# MAGIC Trois propriétés doivent tenir, sans quoi les métriques ne signifieraient rien.

# COMMAND ----------

train, validation, test = datasets["train"], datasets["validation"], datasets["test"]

errors = []

# 1. Aucun chevauchement temporel entre les blocs.
if train.context["event_time"].max() >= validation.context["event_time"].min():
    errors.append("chevauchement train / validation")
if validation.context["event_time"].max() >= test.context["event_time"].min():
    errors.append("chevauchement validation / test")

# 2. Conservation du volume.
total = sum(d.n_rows for d in datasets.values())
if total != len(pdf):
    errors.append(f"conservation : {total:,} != {len(pdf):,}")

# 3. Chaque bloc contient des fraudes — sans quoi il serait inévaluable.
for dataset in datasets.values():
    if dataset.n_frauds == 0:
        errors.append(f"aucune fraude dans le bloc {dataset.name}")

if errors:
    raise ValueError("Protocole invalide :\n" + "\n".join(f"  - {e}" for e in errors))

print("Découpage chronologique valide : aucun chevauchement, volume conservé.")
print(f"  train      : jusqu'au {train.context['event_time'].max()}")
print(f"  validation : jusqu'au {validation.context['event_time'].max()}")
print(f"  test       : jusqu'au {test.context['event_time'].max()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Comparaison de quatre modèles
# MAGIC
# MAGIC Tous reçoivent **exactement les mêmes données** et le même rééquilibrage
# MAGIC (SMOTE à 5 % de minorité). Seul l'algorithme change — c'est la condition pour
# MAGIC que la comparaison signifie quelque chose.
# MAGIC
# MAGIC | Modèle | Rôle dans la comparaison |
# MAGIC |---|---|
# MAGIC | **Régression logistique** | Référence. Si un modèle complexe ne la bat pas nettement, sa complexité n'est pas justifiée. |
# MAGIC | **Forêt aléatoire** | Non linéaire, sans boosting. Isole l'apport du boosting. |
# MAGIC | **LightGBM** | Boosting concurrent, plus rapide sur gros volumes. |
# MAGIC | **XGBoost** | Le modèle du mémoire. Doit être **choisi**, pas subi. |
# MAGIC
# MAGIC ### Le critère de sélection : AUC-PR, pas AUC-ROC
# MAGIC
# MAGIC À 0,4 % de positifs, l'AUC-ROC est dominée par la capacité à reconnaître la
# MAGIC classe majoritaire et reste élevée même quand la précision est catastrophique.
# MAGIC Un modèle à 0,98 d'AUC-ROC peut produire vingt fausses alertes par fraude
# MAGIC détectée.
# MAGIC
# MAGIC L'**AUC-PR** ne considère que la classe positive et s'effondre dès que les
# MAGIC faux positifs augmentent. C'est elle qui mesure ce que le mémoire cherche à
# MAGIC optimiser.

# COMMAND ----------

SMOTE_RATIO = 0.05  # la minorité représentera 5 % de la majorité après SMOTE

MODELS = {
    "logistic_regression": (
        LogisticRegression(max_iter=300, n_jobs=-1, random_state=RANDOM_STATE),
        True,  # mise à l'échelle nécessaire
    ),
    "random_forest": (
        RandomForestClassifier(
            n_estimators=100,
            max_depth=14,
            min_samples_leaf=50,
            max_features="sqrt",
            n_jobs=-1,
            random_state=RANDOM_STATE,
        ),
        False,
    ),
    "lightgbm": (
        LGBMClassifier(
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=64,
            n_jobs=-1,
            random_state=RANDOM_STATE,
            verbose=-1,
        ),
        False,
    ),
    "xgboost": (
        XGBClassifier(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            n_jobs=-1,
            random_state=RANDOM_STATE,
            eval_metric="aucpr",
            tree_method="hist",
        ),
        False,
    ),
}

# COMMAND ----------

def log_model_artifact(model, x_sample, y_sample_pred, artifact_name="model"):
    """Journalise un modèle scikit-learn dans MLflow.

    Le paramètre nommant l'artefact a changé entre les versions majeures de
    MLflow (`artifact_path` puis `name`). On tente la forme récente et on
    retombe sur l'ancienne, pour que le notebook reste exécutable quelle que
    soit la version fournie par le runtime.
    """
    signature = infer_signature(x_sample, y_sample_pred)
    try:
        return mlflow.sklearn.log_model(
            model, name=artifact_name, signature=signature, input_example=x_sample
        )
    except TypeError:
        return mlflow.sklearn.log_model(
            model,
            artifact_path=artifact_name,
            signature=signature,
            input_example=x_sample,
        )


def train_and_evaluate(name, estimator, scale, sampler, run_tags=None):
    """Entraîne un pipeline complet et l'évalue sur le bloc de validation.

    Le sampler fait partie du pipeline : `imblearn` ne l'applique qu'à
    l'entraînement, jamais lors d'une prédiction. C'est cette garantie
    structurelle qui protège la validation de toute contamination.
    """
    steps = [("preprocessing", modeling.build_preprocessor(scale=scale))]
    if sampler is not None:
        steps.append(("rebalancing", sampler))
    steps.append(("classifier", estimator))
    pipeline = ImbPipeline(steps)

    with mlflow.start_run(run_name=name) as run:
        started = time.time()
        pipeline.fit(train.X, train.y)
        duration = time.time() - started

        scores = pipeline.predict_proba(validation.X)[:, 1]
        metrics = modeling.evaluate(validation.y, scores, threshold=0.5)

        mlflow.log_params(
            {
                "algorithm": name,
                "n_features": len(modeling.ALL_FEATURES),
                "n_train_rows": train.n_rows,
                "train_fraud_rate": round(train.fraud_rate, 6),
                "rebalancing": type(sampler).__name__ if sampler else "none",
                "smote_ratio": getattr(sampler, "sampling_strategy", None),
                "scaled": scale,
            }
        )
        mlflow.log_metrics({**metrics, "fit_seconds": duration})
        mlflow.set_tags(run_tags or {})
        log_model_artifact(pipeline, validation.X.head(5), scores[:5])

        print(f"  {name:<22} AUC-PR {metrics['auc_pr']:.4f}   ({duration:.0f} s)")
        return {"model": name, "pipeline": pipeline, "run_id": run.info.run_id, **metrics}


# COMMAND ----------

# MAGIC %md
# MAGIC L'entraînement des quatre modèles prend plusieurs minutes — la forêt
# MAGIC aléatoire est de loin la plus lente sur ce volume.

# COMMAND ----------

results = []
for name, (estimator, scale) in MODELS.items():
    sampler = SMOTE(sampling_strategy=SMOTE_RATIO, random_state=RANDOM_STATE, n_jobs=-1)
    results.append(
        train_and_evaluate(name, estimator, scale, sampler, {"phase": "comparaison"})
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Résultats sur le bloc de validation
# MAGIC
# MAGIC Le seuil est fixé à 0,5 pour cette comparaison — valeur arbitraire, qui sera
# MAGIC optimisée à l'étape 5. Les colonnes `AUC-PR` et `AUC-ROC` en sont
# MAGIC indépendantes ; `Précision`, `Rappel`, `F1` et `TFP` en dépendent et ne sont
# MAGIC à lire que comme un ordre de grandeur.

# COMMAND ----------

print(modeling.format_comparison(results))

best = max(results, key=lambda r: r["auc_pr"])
baseline = next(r for r in results if r["model"] == "logistic_regression")

print()
print(f"Meilleur modèle : {best['model']} (AUC-PR {best['auc_pr']:.4f})")
print(
    f"Gain sur la référence logistique : "
    f"{100 * (best['auc_pr'] - baseline['auc_pr']) / baseline['auc_pr']:+.1f} %"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Le rééquilibrage : quatre stratégies comparées
# MAGIC
# MAGIC C'est ici que se joue une part importante de la problématique.
# MAGIC
# MAGIC | Stratégie | Principe |
# MAGIC |---|---|
# MAGIC | **Aucune** | Le modèle apprend sur la distribution réelle (0,5 % de fraudes) |
# MAGIC | **`scale_pos_weight`** | Pondère les erreurs sur la classe minoritaire, sans créer d'exemples |
# MAGIC | **SMOTE 2 %** | Interpolation d'exemples synthétiques, rééquilibrage léger |
# MAGIC | **SMOTE 20 %** | Rééquilibrage marqué |
# MAGIC
# MAGIC Attendu : plus le rééquilibrage est fort, plus le rappel monte et plus la
# MAGIC précision chute — donc plus les faux positifs augmentent. La question n'est
# MAGIC pas « quel est le meilleur rééquilibrage ? » mais **« quel arbitrage
# MAGIC accepte-t-on ? »**, et c'est un choix métier qui doit être documenté.

# COMMAND ----------

n_negative = int((train.y == 0).sum())
n_positive = int((train.y == 1).sum())
scale_pos_weight = n_negative / n_positive

print(f"Déséquilibre du bloc d'entraînement : {n_negative:,} / {n_positive:,} = {scale_pos_weight:.0f} : 1")

def xgb(**overrides):
    params = dict(
        n_estimators=300, learning_rate=0.05, max_depth=6,
        subsample=0.8, colsample_bytree=0.8, n_jobs=-1,
        random_state=RANDOM_STATE, eval_metric="aucpr", tree_method="hist",
    )
    params.update(overrides)
    return XGBClassifier(**params)


REBALANCING_STRATEGIES = {
    "xgb_sans_reequilibrage": (xgb(), None),
    "xgb_scale_pos_weight": (xgb(scale_pos_weight=scale_pos_weight), None),
    "xgb_smote_02": (xgb(), SMOTE(sampling_strategy=0.02, random_state=RANDOM_STATE, n_jobs=-1)),
    "xgb_smote_20": (xgb(), SMOTE(sampling_strategy=0.20, random_state=RANDOM_STATE, n_jobs=-1)),
}

rebalancing_results = []
for name, (estimator, sampler) in REBALANCING_STRATEGIES.items():
    rebalancing_results.append(
        train_and_evaluate(name, estimator, False, sampler, {"phase": "reequilibrage"})
    )

# COMMAND ----------

print(modeling.format_comparison(rebalancing_results))

print()
print("Lecture : à seuil constant, un rééquilibrage plus fort augmente le rappel")
print("et dégrade la précision. Le taux de faux positifs (TFP) en est la mesure")
print("directe — c'est le KPI de la problématique.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Modèle retenu et enregistrement
# MAGIC
# MAGIC Le modèle final est sélectionné sur l'**AUC-PR mesurée en validation**, une
# MAGIC métrique indépendante du seuil. Le seuil sera choisi séparément à l'étape 5,
# MAGIC également sur la validation — le bloc de test reste intact jusqu'à
# MAGIC l'évaluation finale.

# COMMAND ----------

all_results = results + rebalancing_results
final = max(all_results, key=lambda r: r["auc_pr"])

print(f"Modèle retenu : {final['model']}")
print(f"  AUC-PR (validation)  : {final['auc_pr']:.4f}")
print(f"  AUC-ROC (validation) : {final['auc_roc']:.4f}")
print(f"  Identifiant du run   : {final['run_id']}")

# COMMAND ----------

model_uri = f"runs:/{final['run_id']}/model"

registered = mlflow.register_model(
    model_uri=model_uri,
    name=config.REGISTERED_MODEL_NAME,
    tags={"selection_metric": "auc_pr", "algorithm": final["model"]},
)

print(f"Modèle enregistré : {config.REGISTERED_MODEL_NAME}  version {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Découpage | Chronologique, 3 blocs sans chevauchement |
# MAGIC | Modèles comparés | 4 algorithmes + 4 stratégies de rééquilibrage |
# MAGIC | Critère de sélection | AUC-PR sur le bloc de validation |
# MAGIC | Suivi | MLflow — paramètres, métriques et modèles de chaque run |
# MAGIC | Registre | Unity Catalog |
# MAGIC | Bloc de test | **jamais consulté** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Points à savoir expliquer en soutenance
# MAGIC
# MAGIC 1. **Pourquoi trois blocs et pas deux ?** Choisir le seuil sur le test
# MAGIC    reviendrait à l'optimiser sur les données censées mesurer la performance.
# MAGIC    Le taux de faux positifs annoncé serait optimiste — or c'est le chiffre
# MAGIC    central du mémoire.
# MAGIC 2. **Pourquoi l'AUC-PR et pas l'AUC-ROC ?** À 0,4 % de positifs, l'AUC-ROC
# MAGIC    reste élevée même quand la précision s'effondre. L'AUC-PR ne regarde que
# MAGIC    la classe positive : c'est elle qui mesure les faux positifs.
# MAGIC 3. **Pourquoi ne pas rééquilibrer à 50/50 ?** Parce que cela apprend au
# MAGIC    modèle que la fraude est un événement sur deux. Il devient hypersensible
# MAGIC    et noie les analystes sous les fausses alertes — l'inverse de l'objectif.
# MAGIC 4. **Pourquoi SMOTE dans un pipeline `imblearn` ?** Parce qu'il garantit
# MAGIC    structurellement que le rééquilibrage ne s'applique jamais à la
# MAGIC    validation ni au test. Appliqué à la main, il finit par contaminer
# MAGIC    l'évaluation, et l'erreur est invisible dans les métriques.
# MAGIC 5. **Pourquoi une régression logistique dans la comparaison ?** Pour
# MAGIC    justifier la complexité. Si XGBoost ne la bat pas nettement, sa
# MAGIC    complexité n'est pas défendable.
# MAGIC
# MAGIC **Étape suivante** : `05_scoring_eval` — optimisation du seuil de décision,
# MAGIC explicabilité SHAP, évaluation finale sur le bloc de test.
