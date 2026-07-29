"""Tests des métriques d'évaluation.

Ce module est celui où une erreur coûterait le plus cher : une définition
fausse du taux de faux positifs ne provoque aucune exception, ne se voit pas à
la lecture d'un tableau de résultats, et invaliderait silencieusement toutes
les conclusions du mémoire.

La stratégie de test est donc la **vérification croisée** : chaque métrique est
comparée à un calcul indépendant réalisé directement depuis la matrice de
confusion de scikit-learn. On ne teste pas que le code s'exécute, on teste
qu'il calcule la bonne chose.

Exécution :
    py -3.13 -m pytest tests/ -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import confusion_matrix

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from fraud.metrics import (  # noqa: E402
    best_threshold_for_recall,
    evaluate,
    threshold_sweep,
)

# Le jeu synthétique reproduit le déséquilibre réel du corpus (~0,4 % de
# fraudes) : tester sur des classes équilibrées masquerait précisément les
# défauts que ces métriques doivent révéler.
N_ROWS = 50_000
FRAUD_RATE = 0.004
THRESHOLDS = [0.2, 0.35, 0.5, 0.7, 0.9]


@pytest.fixture(scope="module")
def scored_sample() -> tuple[pd.Series, np.ndarray]:
    """Cible déséquilibrée et scores d'un classifieur imparfait."""
    rng = np.random.default_rng(42)
    y = pd.Series((rng.random(N_ROWS) < FRAUD_RATE).astype(int))
    # Scores corrélés à la cible mais avec recouvrement : sans recouvrement,
    # tous les seuils donneraient le même résultat et les tests ne prouveraient
    # rien.
    scores = np.clip(rng.beta(2, 8, N_ROWS) + y * rng.normal(0.45, 0.15, N_ROWS), 0, 1)
    return y, scores


@pytest.mark.parametrize("threshold", THRESHOLDS)
def test_evaluate_matches_confusion_matrix(scored_sample, threshold):
    """Chaque compte et chaque taux coïncide avec un calcul direct."""
    y, scores = scored_sample
    metrics = evaluate(y, scores, threshold=threshold)
    tn, fp, fn, tp = confusion_matrix(
        y, (scores >= threshold).astype(int), labels=[0, 1]
    ).ravel()

    assert metrics["true_positives"] == tp
    assert metrics["false_positives"] == fp
    assert metrics["false_negatives"] == fn
    assert metrics["true_negatives"] == tn
    assert metrics["precision"] == pytest.approx(tp / (tp + fp))
    assert metrics["recall"] == pytest.approx(tp / (tp + fn))


@pytest.mark.parametrize("threshold", THRESHOLDS)
def test_false_positive_rate_denominator_is_legitimate_traffic(scored_sample, threshold):
    """Le taux de faux positifs se rapporte aux légitimes, pas au total.

    C'est la définition métier : « quelle proportion des clients honnêtes
    bloque-t-on à tort ? ». Rapportée au total, la valeur serait diluée par la
    classe majoritaire et paraîtrait systématiquement meilleure qu'elle ne l'est
    — un biais d'autant plus trompeur que le KPI est central au mémoire.
    """
    y, scores = scored_sample
    metrics = evaluate(y, scores, threshold=threshold)
    tn, fp, _, _ = confusion_matrix(
        y, (scores >= threshold).astype(int), labels=[0, 1]
    ).ravel()

    assert metrics["false_positive_rate"] == pytest.approx(fp / (fp + tn))
    # Et surtout : ce n'est PAS le rapport au total.
    if fp > 0:
        assert metrics["false_positive_rate"] != pytest.approx(fp / len(y))


def test_alerts_per_1000_counts_every_alert(scored_sample):
    """Une alerte est une prédiction positive, juste ou fausse.

    L'analyste traite les vrais comme les faux positifs : la charge de travail
    se mesure sur le total des alertes.
    """
    y, scores = scored_sample
    metrics = evaluate(y, scores, threshold=0.5)
    expected = 1000 * (metrics["true_positives"] + metrics["false_positives"]) / len(y)
    assert metrics["alerts_per_1000"] == pytest.approx(expected)


def test_threshold_sweep_reconstructs_false_positives(scored_sample):
    """Les faux positifs déduits de la courbe égalent ceux réellement observés.

    `threshold_sweep` ne recalcule pas une matrice de confusion par seuil — ce
    serait prohibitif sur des dizaines de milliers de seuils. Il reconstitue les
    faux positifs depuis la précision et le rappel. Ce test valide cette
    reconstruction contre la vérité terrain.
    """
    y, scores = scored_sample
    sweep = threshold_sweep(y, scores, min_recall=0.05)
    assert not sweep.empty

    for _, row in sweep.iloc[:: max(1, len(sweep) // 10)].iterrows():
        _, fp, _, _ = confusion_matrix(
            y, (scores >= row["threshold"]).astype(int), labels=[0, 1]
        ).ravel()
        # Tolérance de 1 : la reconstruction passe par un arrondi.
        assert abs(row["false_positives"] - fp) <= 1


def test_threshold_sweep_respects_recall_floor(scored_sample):
    """Le filtre de rappel minimal est appliqué.

    Sans plancher, le seuil le plus « précis » serait celui qui ne détecte
    presque rien — précision parfaite, rappel négligeable, modèle inutile.
    """
    y, scores = scored_sample
    assert (threshold_sweep(y, scores, min_recall=0.60)["recall"] >= 0.60).all()


def test_raising_recall_target_increases_false_positives(scored_sample):
    """Détecter plus de fraudes coûte davantage de fausses alertes.

    C'est l'arbitrage central de la problématique. Le test vérifie que le
    dispositif de mesure le restitue : si les faux positifs n'augmentaient pas
    avec le rappel visé, c'est que la courbe serait mal construite.
    """
    y, scores = scored_sample
    sweep = threshold_sweep(y, scores)

    false_positives = [
        best_threshold_for_recall(sweep, target)["false_positives"]
        for target in (0.60, 0.75, 0.90)
    ]
    assert false_positives == sorted(false_positives)


def test_best_threshold_for_recall_rejects_unreachable_target(scored_sample):
    """Un rappel inatteignable lève une erreur explicite plutôt que de renvoyer
    silencieusement un seuil qui ne le respecte pas."""
    y, scores = scored_sample
    sweep = threshold_sweep(y, scores, min_recall=0.90)
    with pytest.raises(ValueError, match="rappel"):
        best_threshold_for_recall(sweep, target_recall=1.01)


def test_perfect_classifier_has_no_false_positives():
    """Cas limite : un classifieur parfait doit afficher un taux de faux
    positifs nul et une précision de 1."""
    y = pd.Series([0] * 990 + [1] * 10)
    scores = np.array([0.01] * 990 + [0.99] * 10)

    metrics = evaluate(y, scores, threshold=0.5)
    assert metrics["false_positives"] == 0
    assert metrics["false_positive_rate"] == 0.0
    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 1.0
    assert metrics["auc_pr"] == pytest.approx(1.0)


def test_threshold_above_every_score_predicts_nothing():
    """Cas limite : aucun positif prédit. Les métriques valent 0 sans division
    par zéro — un seuil mal choisi ne doit pas faire tomber le pipeline."""
    y = pd.Series([0] * 990 + [1] * 10)
    scores = np.array([0.01] * 990 + [0.99] * 10)

    metrics = evaluate(y, scores, threshold=1.5)
    assert metrics["true_positives"] == 0
    assert metrics["false_positives"] == 0
    assert metrics["precision"] == 0.0
    assert metrics["recall"] == 0.0
    assert metrics["f1"] == 0.0
    assert metrics["alerts_per_1000"] == 0.0
