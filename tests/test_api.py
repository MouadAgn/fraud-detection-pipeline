"""Contrat HTTP et prédictions réelles sur un petit modèle synthétique de test."""
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sklearn.linear_model import LogisticRegression

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fraud_api.app import Scorer, create_app, load_bundle


@pytest.fixture
def bundle(tmp_path):
    import mlflow.sklearn

    frame = pd.DataFrame({"amount": [1., 2., 50., 100.]})
    model = LogisticRegression().fit(frame, [0, 0, 1, 1])
    mlflow.sklearn.save_model(model, str(tmp_path / "model"), serialization_format="cloudpickle")
    data = frame.assign(transaction_id=["001", "002", "003", "004"])
    data.to_json(tmp_path / "transactions.json", orient="table", index=False)
    metadata = {"schema_version": 1, "model_name": "test-only", "model_version": "7",
                "decision_threshold": 0.5, "feature_columns": ["amount"],
                "features_snapshot": "2026-10-22T00:00:00Z"}
    (tmp_path / "metadata.json").write_text(json.dumps(metadata))
    return tmp_path, model, frame


def test_health_without_model_and_no_fake_prediction(tmp_path):
    with TestClient(create_app(bundle_path=tmp_path / "absent")) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert client.get("/ready").status_code == 503
        assert client.post("/predict", json={"transaction_id": "001"}).status_code == 503


def test_local_mlflow_bundle_matches_original_model(bundle):
    path, model, frame = bundle
    with TestClient(create_app(bundle_path=path)) as client:
        assert client.get("/ready").status_code == 200
        result = client.post("/predict", json={"transaction_id": "001"})
        assert result.status_code == 200
        data = result.json()
        assert data["fraud_score"] == pytest.approx(model.predict_proba(frame.iloc[[0]])[0, 1])
        assert data["is_flagged"] is False
        assert data["model_version"] == "7"
        assert data["transaction_id"] == "001"
        assert client.post("/predict", json={"transaction_id": "absent"}).status_code == 404
        assert client.post("/predict", json={"transaction_id": ""}).status_code == 422
        assert client.post("/predict", json={"transaction_id": "001", "amount": 20}).status_code == 422


def test_threshold_inclusive(bundle):
    path, _, _ = bundle
    scorer = load_bundle(path)
    scorer.metadata["decision_threshold"] = scorer.predict("001").fraud_score
    assert scorer.predict("001").is_flagged is True


def test_corrupt_bundle_is_not_ready(tmp_path):
    (tmp_path / "metadata.json").write_text("{}")
    with TestClient(create_app(bundle_path=tmp_path)) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/ready").status_code == 503


def test_duplicate_transactions_rejected(bundle):
    path, model, frame = bundle
    metadata = json.loads((path / "metadata.json").read_text())
    with pytest.raises(ValueError, match="dupliqués"):
        Scorer(model, frame.assign(transaction_id="same"), metadata)


def test_invalid_model_output_returns_generic_error(bundle):
    path, _, _ = bundle
    scorer = load_bundle(path)
    scorer.model.predict_proba = lambda frame: np.array([[0., np.nan]])
    with TestClient(create_app(scorer=scorer)) as client:
        result = client.post("/predict", json={"transaction_id": "001"})
        assert result.status_code == 500
        assert result.json()["detail"] == "Le calcul du score a échoué."
