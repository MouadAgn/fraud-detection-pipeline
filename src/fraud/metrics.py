"""Métriques d'évaluation adaptées au déséquilibre de classes.

Module volontairement **sans dépendance à Spark** : les définitions de métriques
sont la partie du code où une erreur fausserait silencieusement toutes les
conclusions du mémoire, et c'est donc celle qui doit être couverte par des
tests. En l'isolant, on peut l'exécuter et la vérifier hors de tout
environnement Databricks (voir ``tests/test_metrics.py``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
)


def evaluate(y_true: pd.Series, y_score: np.ndarray, threshold: float = 0.5) -> dict:
    """Calcule les métriques d'un modèle à un seuil de décision donné.

    **Pourquoi l'AUC-ROC ne suffit pas.** À 0,4 % de positifs, elle est dominée
    par la capacité du modèle à reconnaître la classe majoritaire et reste
    élevée même quand la précision est catastrophique. Un modèle affichant 0,98
    d'AUC-ROC peut très bien produire vingt fausses alertes par fraude détectée.

    L'**AUC-PR** (précision moyenne) ne considère que la classe positive et
    s'effondre dès que les faux positifs augmentent. C'est elle qui sert de
    critère de sélection, parce qu'elle mesure ce que le mémoire cherche à
    optimiser.

    Deux indicateurs métier complètent le tableau :

    - ``false_positive_rate`` — part des transactions **légitimes** bloquées à
      tort. Le dénominateur est le nombre de transactions légitimes, non le
      total : rapportée au total, la valeur serait diluée par la classe
      majoritaire et perdrait son sens opérationnel.
    - ``alerts_per_1000`` — nombre d'alertes à traiter pour mille transactions.
      Traduit le taux de faux positifs en charge de travail réelle pour une
      équipe d'analystes, ce qu'aucun pourcentage ne rend intuitif.
    """
    y_pred = (np.asarray(y_score) >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        "threshold": float(threshold),
        "auc_roc": float(roc_auc_score(y_true, y_score)),
        "auc_pr": float(average_precision_score(y_true, y_score)),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "false_positive_rate": float(fp / (fp + tn)) if (fp + tn) else 0.0,
        "alerts_per_1000": float(1000 * (tp + fp) / len(y_true)),
        "true_positives": int(tp),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_negatives": int(tn),
    }


def threshold_sweep(
    y_true: pd.Series, y_score: np.ndarray, min_recall: float = 0.0
) -> pd.DataFrame:
    """Balaie les seuils de décision et mesure l'arbitrage à chacun.

    C'est le matériau central de l'étape 5. Pour chaque seuil, la courbe donne
    le couple (fraudes détectées, fausses alertes générées) — et c'est ce
    couple, non le modèle, qui détermine le taux de faux positifs.

    Le nombre de faux positifs est reconstitué depuis la précision et le rappel
    plutôt que recalculé seuil par seuil : ``precision_recall_curve`` évalue des
    dizaines de milliers de seuils en une passe, là où autant de matrices de
    confusion coûteraient plusieurs minutes.

    Args:
        min_recall: rappel plancher. Filtrer les seuils qui ne l'atteignent pas
            évite de retenir un point où la précision paraît excellente
            simplement parce que le modèle ne détecte presque rien.
    """
    precision, recall, thresholds = precision_recall_curve(y_true, y_score)
    # `precision_recall_curve` renvoie un point de plus que de seuils.
    sweep = pd.DataFrame(
        {"threshold": thresholds, "precision": precision[:-1], "recall": recall[:-1]}
    )
    sweep["f1"] = (
        2 * sweep["precision"] * sweep["recall"] / (sweep["precision"] + sweep["recall"])
    ).fillna(0.0)

    n_positive = int(np.asarray(y_true).sum())
    n_negative = len(y_true) - n_positive

    detected = sweep["recall"] * n_positive  # vrais positifs
    alerts = detected / sweep["precision"].replace(0, np.nan)  # vrais + faux positifs
    sweep["false_positives"] = (alerts - detected).round()
    sweep["false_positive_rate"] = sweep["false_positives"] / n_negative

    return sweep[sweep["recall"] >= min_recall].reset_index(drop=True)


def best_threshold_for_recall(sweep: pd.DataFrame, target_recall: float) -> pd.Series:
    """Seuil le plus précis parmi ceux atteignant un rappel donné.

    Formule l'arbitrage dans le sens métier : « je veux détecter au moins X %
    des fraudes — quel est le seuil qui minimise alors les fausses alertes ? ».
    """
    candidates = sweep[sweep["recall"] >= target_recall]
    if candidates.empty:
        raise ValueError(f"Aucun seuil n'atteint un rappel de {target_recall:.0%}.")
    return candidates.loc[candidates["precision"].idxmax()]


def cost_curve(
    y_true: pd.Series,
    y_score: np.ndarray,
    amounts: pd.Series,
    false_alert_cost: float,
) -> pd.DataFrame:
    """Coût métier total en fonction du seuil de décision.

    Le seuil ne devrait pas se choisir sur une métrique statistique. Maximiser
    le F1 revient à postuler qu'un faux positif et un faux négatif se valent —
    ce qui est faux dans tous les métiers, et particulièrement en fraude, où
    les deux erreurs n'ont ni la même nature ni le même coût :

    - **Faux négatif** — une fraude non détectée. Son coût est le **montant de
      la transaction**, présent dans les données : il n'a pas à être supposé.
    - **Faux positif** — une transaction légitime bloquée. Son coût est fixe et
      composite (temps d'analyste, contact client, friction commerciale), et
      doit être posé en hypothèse.

    Le seuil optimal est celui qui minimise la somme des deux. Il dépend donc
    de l'hypothèse de coût, ce qui impose une analyse de sensibilité plutôt
    qu'une valeur unique présentée comme la vérité.

    La courbe est calculée en une passe par sommes cumulées sur les scores
    triés, et non par un balayage de seuils : le coût passe de quadratique à
    ``O(n log n)``.

    Returns:
        Une ligne par seuil candidat, du plus sélectif au plus permissif. La
        première ligne correspond à l'absence totale d'alerte — la référence
        « aucun modèle », où toute la fraude est subie.
    """
    y = np.asarray(y_true)
    scores = np.asarray(y_score)
    values = np.asarray(amounts, dtype=float)

    # Tri stable : à scores égaux, l'ordre d'entrée est préservé. Un tri
    # instable réordonnerait arbitrairement les ex æquo, et la courbe
    # changerait d'une exécution à l'autre sans qu'aucune donnée n'ait bougé.
    order = np.argsort(-scores, kind="stable")  # du plus suspect au moins suspect
    y, scores, values = y[order], scores[order], values[order]

    # Alerter sur les k transactions les mieux classées revient à fixer le
    # seuil à la valeur du k-ième score.
    detected_amount = np.concatenate([[0.0], np.cumsum(values * y)])
    true_positives = np.concatenate([[0], np.cumsum(y)])
    false_positives = np.concatenate([[0], np.cumsum(1 - y)])
    thresholds = np.concatenate([[np.inf], scores])

    total_fraud_amount = detected_amount[-1]
    missed_amount = total_fraud_amount - detected_amount
    alert_cost = false_alert_cost * false_positives

    n_frauds = max(int(y.sum()), 1)
    alerts = np.arange(len(thresholds))

    curve = pd.DataFrame(
        {
            "threshold": thresholds,
            "alerts": alerts,
            "true_positives": true_positives,
            "false_positives": false_positives,
            "fraud_amount_recovered": detected_amount,
            "fraud_amount_missed": missed_amount,
            "alert_cost": alert_cost,
            "total_cost": missed_amount + alert_cost,
            "recall": true_positives / n_frauds,
        }
    )
    curve["precision"] = np.divide(
        true_positives, alerts, out=np.zeros_like(detected_amount), where=alerts > 0
    )

    # --- Traitement des ex æquo -------------------------------------------
    # Le raisonnement par sommes cumulées décrit « alerter sur les k
    # transactions les mieux classées ». Or la règle réellement appliquée en
    # production est « alerter si score >= seuil » : lorsque plusieurs
    # transactions partagent le même score, elle en déclenche davantage que k.
    #
    # Une coupure au milieu d'un groupe d'ex æquo décrirait donc un point de
    # fonctionnement inatteignable. Seules les coupures séparant deux scores
    # distincts sont conservées, ce qui rend chaque ligne de la courbe
    # exactement reproductible par application du seuil correspondant.
    n = len(scores)
    is_applicable = np.ones(n + 1, dtype=bool)
    if n > 1:
        is_applicable[1:n] = scores[: n - 1] != scores[1:n]

    return curve[is_applicable].reset_index(drop=True)


def optimal_threshold_by_cost(curve: pd.DataFrame, tolerance: float = 1e-9) -> pd.Series:
    """Point de la courbe minimisant le coût métier total.

    **Pourquoi une tolérance et un départage explicite.** Au voisinage du
    minimum, la courbe de coût est très plate : plusieurs seuils voisins
    produisent un coût pratiquement identique. Retenir mécaniquement le premier
    minimum rencontré rendrait le résultat dépendant de variations numériques
    infimes — l'ordre dans lequel les lignes ont été lues, par exemple — et le
    seuil retenu changerait d'une exécution à l'autre sans qu'aucune donnée
    n'ait bougé.

    Deux seuils sont donc considérés comme équivalents lorsque leurs coûts
    diffèrent de moins de ``tolerance`` fois le coût minimal. Parmi eux, on
    retient le **plus sélectif**.

    Ce départage n'est pas arbitraire : à coût métier égal, le seuil le plus
    élevé émet moins d'alertes, donc dérange moins de clients honnêtes et
    charge moins les analystes. C'est le choix que ferait le métier.
    """
    minimum = curve["total_cost"].min()
    equivalent = curve[curve["total_cost"] <= minimum * (1 + tolerance) + tolerance]
    return equivalent.loc[equivalent["threshold"].idxmax()]


def format_comparison(results: list[dict]) -> str:
    """Met en forme le tableau comparatif des modèles."""
    header = (
        f"{'Modèle':<28}{'AUC-PR':>9}{'AUC-ROC':>10}"
        f"{'Précision':>11}{'Rappel':>9}{'F1':>8}{'TFP':>9}"
    )
    lines = [header, "-" * len(header)]
    for row in sorted(results, key=lambda r: r["auc_pr"], reverse=True):
        lines.append(
            f"{row['model']:<28}{row['auc_pr']:>9.4f}{row['auc_roc']:>10.4f}"
            f"{row['precision']:>11.4f}{row['recall']:>9.4f}"
            f"{row['f1']:>8.4f}{row['false_positive_rate']:>9.4f}"
        )
    lines.append("")
    lines.append(
        "TFP = taux de faux positifs — part des transactions légitimes bloquées à tort."
    )
    return "\n".join(lines)
