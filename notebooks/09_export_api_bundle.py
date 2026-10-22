# Databricks notebook source
# MAGIC %md
# MAGIC # Export local pour l'API
# MAGIC Exécuter après `05_scoring_eval`. Ne modifie ni les tables ni l'alias.
# MAGIC L'export contient le modèle complet, son seuil et 100 transactions Gold.
# MAGIC Télécharger ensuite `api_bundle.zip` depuis le volume Unity Catalog.

# COMMAND ----------
import sys
from pathlib import Path

# Même résolution du dossier src que les notebooks du projet.
project_root = Path.cwd()
if not (project_root / "src").exists():
    project_root = project_root.parent
sys.path.insert(0, str(project_root / "src"))

import json
import shutil
import tempfile
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn
import numpy as np
from mlflow import MlflowClient
from fraud import config, modeling

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
version = client.get_model_version_by_alias(config.REGISTERED_MODEL_NAME, config.MODEL_ALIAS)
# Résoudre l'alias une seule fois, puis exporter une version numérique immuable.
model_uri = f"models:/{config.REGISTERED_MODEL_NAME}/{version.version}"
properties = {r.key: r.value for r in spark.sql(
    f"SHOW TBLPROPERTIES {config.GOLD_SCORED_TABLE}"
).collect()}
threshold = float(properties["fraud.decision_threshold"])
# Le seuil de la table scorée doit correspondre au modèle exporté.
scored_uri = properties.get("fraud.model_uri")
if scored_uri not in (model_uri, f"models:/{config.REGISTERED_MODEL_NAME}@{config.MODEL_ALIAS}"):
    raise ValueError("Le modèle de la table scorée ne correspond pas au modèle exporté")
# Un alias est mutable : vérifier aussi la provenance du score en recalculant
# le seuil avec le modèle résolu, selon le même protocole que le notebook 05.
from fraud import metrics
pdf = modeling.to_pandas(modeling.add_temporal_split(spark.table(config.GOLD_FEATURES_TABLE)))
validation = modeling.split_datasets(pdf)["validation"]
model = mlflow.sklearn.load_model(model_uri)
curve = metrics.cost_curve(validation.y, model.predict_proba(validation.X)[:, 1],
                          validation.X["amount"],
                          false_alert_cost=config.FALSE_ALERT_COST)
expected_threshold = float(metrics.optimal_threshold_by_cost(curve)["threshold"])
if not np.isclose(threshold, expected_threshold, rtol=0, atol=1e-12):
    raise ValueError("Seuil incohérent : relancer le notebook 05 avec le champion actuel")

sample = pdf.loc[pdf[modeling.SPLIT_COL] == "test"].head(100)
if sample.empty:
    raise ValueError("Aucune transaction test à exporter")
columns = list(modeling.ALL_FEATURES)
export_dir = Path(tempfile.mkdtemp()) / "api_bundle"
export_dir.mkdir()
# Copier l'artefact original pour conserver les dépendances d'entraînement.
model_dir = mlflow.artifacts.download_artifacts(artifact_uri=model_uri)
shutil.copytree(model_dir, export_dir / "model")
sample[["transaction_id", *columns]].to_json(export_dir / "transactions.json", orient="table", index=False)
metadata = {
    "schema_version": 1,
    "model_name": config.REGISTERED_MODEL_NAME,
    "model_version": str(version.version),
    "decision_threshold": threshold,
    "feature_columns": columns,
    "features_snapshot": datetime.now(timezone.utc).isoformat(),
}
(export_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))
reference = [{"transaction_id": str(t), "fraud_score": float(s)} for t, s in zip(
    sample["transaction_id"], model.predict_proba(sample[columns])[:, 1]
)]
(export_dir / "reference_predictions.json").write_text(json.dumps(reference, indent=2))
archive = shutil.make_archive(str(export_dir.parent / "api_bundle"), "zip", root_dir=export_dir.parent, base_dir="api_bundle")
target = Path(config.VOLUME_PATH) / "api_bundle.zip"
shutil.copyfile(archive, target)
print(f"Export disponible : {target}")
print(f"Premier identifiant pour tester : {sample.iloc[0]['transaction_id']}")
