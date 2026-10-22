"""API de scoring à partir d'un export Databricks local et versionné."""
from contextlib import asynccontextmanager
import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    transaction_id: str = Field(min_length=1, max_length=128)


class PredictionResponse(BaseModel):
    transaction_id: str
    fraud_score: float = Field(ge=0, le=1)
    is_flagged: bool
    decision_threshold: float
    model_name: str
    model_version: str
    features_snapshot: str


class Scorer:
    def __init__(self, model, transactions, metadata):
        self.model = model
        self.metadata = metadata
        self.columns = metadata["feature_columns"]
        threshold = metadata["decision_threshold"]
        if not np.isfinite(threshold) or not 0 <= threshold <= 1:
            raise ValueError("Seuil de décision invalide")
        if not self.columns or len(set(self.columns)) != len(self.columns):
            raise ValueError("Liste de variables invalide")
        if "transaction_id" not in transactions or transactions["transaction_id"].isna().any():
            raise ValueError("Identifiants manquants")
        transactions = transactions.copy()
        transactions["transaction_id"] = transactions["transaction_id"].astype(str)
        if transactions["transaction_id"].duplicated().any():
            raise ValueError("Identifiants dupliqués")
        self.transactions = transactions.set_index("transaction_id")
        self.transactions[self.columns]  # Vérifier le contrat dès le chargement.
        for key in ("model_name", "model_version", "features_snapshot"):
            if not metadata.get(key):
                raise ValueError(f"Métadonnée absente : {key}")
        classes = list(model.classes_)
        if 1 not in classes:
            raise ValueError("Le modèle ne possède pas de classe fraude 1")
        self.fraud_index = classes.index(1)

    def predict(self, transaction_id):
        if transaction_id not in self.transactions.index:
            raise KeyError(transaction_id)
        row = self.transactions.loc[[transaction_id], self.columns]
        score = float(self.model.predict_proba(row)[0, self.fraud_index])
        if not np.isfinite(score) or not 0 <= score <= 1:
            raise ValueError("Score du modèle invalide")
        return PredictionResponse(
            transaction_id=transaction_id,
            fraud_score=score,
            is_flagged=score >= self.metadata["decision_threshold"],
            **{key: self.metadata[key] for key in (
                "decision_threshold", "model_name", "model_version", "features_snapshot"
            )},
        )


def load_bundle(path):
    # Charger uniquement un artefact de confiance : les modèles sklearn
    # sérialisés peuvent exécuter du code lors de leur désérialisation.
    import mlflow.sklearn

    path = Path(path)
    metadata = json.loads((path / "metadata.json").read_text())
    if metadata.get("schema_version") != 1:
        raise ValueError("Version du format d'export inconnue")
    transactions = pd.read_json(path / "transactions.json", orient="table")
    model = mlflow.sklearn.load_model(str(path / "model"))
    return Scorer(model, transactions, metadata)


def create_app(bundle_path=None, scorer=None):
    @asynccontextmanager
    async def lifespan(app):
        app.state.scorer = scorer
        path = bundle_path or os.getenv("FRAUD_BUNDLE_PATH", "data/api_bundle")
        if app.state.scorer is None and Path(path).exists():
            try:
                app.state.scorer = load_bundle(path)
            except Exception:
                logger.exception("Impossible de charger l'export du modèle")
        yield
        app.state.scorer = None

    app = FastAPI(
        title="Fraud Detection API",
        description="Démonstrateur local sur un export Gold. Ne déclenche pas le pipeline Databricks.",
        version="0.1.0", lifespan=lifespan,
    )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/ready")
    def ready():
        current = app.state.scorer
        if current is None:
            raise HTTPException(503, "Modèle et données non chargés. Export Databricks requis.")
        return {"status": "ready", "model_version": current.metadata["model_version"]}

    @app.post("/predict", response_model=PredictionResponse)
    def predict(request: PredictionRequest):
        current = app.state.scorer
        if current is None:
            raise HTTPException(503, "Modèle et données non chargés. Export Databricks requis.")
        try:
            return current.predict(request.transaction_id)
        except KeyError:
            raise HTTPException(404, "Transaction absente de l'échantillon exporté.") from None
        except Exception:
            logger.exception("Échec du calcul de prédiction")
            raise HTTPException(500, "Le calcul du score a échoué.") from None

    return app


app = create_app()
