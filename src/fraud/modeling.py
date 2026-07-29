"""Préparation des données et évaluation des modèles (étapes 4 et 5).

Ce module ne contient aucun modèle : il contient les décisions
**méthodologiques** qui déterminent si les métriques obtenues signifient
quelque chose. Le choix de l'algorithme est secondaire face au protocole
d'évaluation — un XGBoost évalué sur un découpage aléatoire produit des
chiffres plus flatteurs et beaucoup moins fiables qu'une régression logistique
évaluée correctement.

Trois décisions y sont encodées :

1. **Découpage chronologique à trois blocs** — entraînement, validation, test.
2. **Traitement uniforme des valeurs manquantes** — imputation médiane assortie
   d'un indicateur, pour que les quatre modèles comparés voient exactement la
   même information.
3. **Métriques adaptées au déséquilibre** — l'AUC-ROC seule est trompeuse à
   0,5 % de positifs ; l'AUC-PR et le taux de faux positifs la complètent.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from fraud import config, features

# Les métriques vivent dans un module sans dépendance à Spark, pour être
# couvertes par des tests exécutables hors Databricks. Elles sont réexportées
# ici pour que les notebooks n'aient qu'un import à faire.
from fraud.metrics import (  # noqa: F401  (réexport volontaire)
    best_threshold_for_recall,
    evaluate,
    format_comparison,
    threshold_sweep,
)

TARGET = "is_fraud"
SPLIT_COL = "split"

# Colonnes conservées hors variables explicatives : elles servent à auditer,
# joindre et analyser les résultats, jamais à prédire.
CONTEXT_COLUMNS: tuple[str, ...] = ("transaction_id", "customer_id", "event_time")

# ---------------------------------------------------------------------------
# Typage des variables explicatives
# ---------------------------------------------------------------------------
CATEGORICAL_FEATURES: tuple[str, ...] = (
    "merchant_category",
    "customer_gender",
    "customer_state",
)

BOOLEAN_FEATURES: tuple[str, ...] = (
    "is_new_merchant_for_customer",
    "is_new_category_for_customer",
    "is_weekend",
    "is_night",
)

# Encodage cyclique de l'heure. `hour_of_day` est un entier de 0 à 23, mais 23 h
# et 0 h sont voisines dans la réalité et distantes de 23 unités pour un modèle
# linéaire. La projection sur un cercle rétablit cette proximité.
#
# Les modèles à arbres n'en ont pas besoin — ils découpent l'axe par seuils et
# retrouveraient la plage nocturne d'eux-mêmes. Mais la comparaison doit être
# équitable : sans cet encodage, la régression logistique de référence serait
# handicapée par la représentation plutôt que par ses capacités, et sa
# contre-performance ne prouverait rien.
CYCLICAL_FEATURES: tuple[str, ...] = ("hour_sin", "hour_cos")

NUMERIC_FEATURES: tuple[str, ...] = tuple(
    column
    for column in features.FEATURE_COLUMNS
    if column not in CATEGORICAL_FEATURES
) + CYCLICAL_FEATURES

ALL_FEATURES: tuple[str, ...] = NUMERIC_FEATURES + CATEGORICAL_FEATURES


# ---------------------------------------------------------------------------
# Découpage chronologique
# ---------------------------------------------------------------------------
def add_temporal_split(df: DataFrame) -> DataFrame:
    """Étiquette chaque transaction en train / validation / test.

    Les frontières sont des dates, non des fichiers d'origine. Le résultat
    coïncide ici avec le découpage fourni par Kaggle, mais la logique reste
    valable si le corpus est étendu ou rejoué.
    """
    event_time = F.col("event_time")
    return df.withColumn(
        SPLIT_COL,
        F.when(event_time < F.lit(config.TRAIN_END), F.lit("train"))
        .when(event_time < F.lit(config.VALIDATION_END), F.lit("validation"))
        .otherwise(F.lit("test")),
    )


def to_pandas(df: DataFrame) -> pd.DataFrame:
    """Rapatrie la table Gold en mémoire, en maîtrisant l'empreinte.

    Le volume — 1,85 M lignes pour une trentaine de colonnes — tient
    confortablement sur un nœud unique après conversion en simple précision.
    L'entraînement distribué (SparkXGBoost) n'apporterait ici aucun gain et
    ajouterait une complexité que rien ne justifie. Le seuil au-delà duquel il
    deviendrait nécessaire se situe plutôt vers la dizaine de millions de lignes.
    """
    columns = list(CONTEXT_COLUMNS) + [SPLIT_COL, TARGET] + [
        column for column in features.FEATURE_COLUMNS
    ]
    pdf = df.select(*columns).toPandas()

    # Simple précision : divise par deux l'empreinte mémoire sans effet
    # mesurable sur des modèles à arbres.
    for column in pdf.select_dtypes(include=["float64"]).columns:
        pdf[column] = pdf[column].astype("float32")
    for column in BOOLEAN_FEATURES:
        pdf[column] = pdf[column].astype("int8")

    # Encodage cyclique de l'heure (voir CYCLICAL_FEATURES).
    angle = 2 * np.pi * pdf["hour_of_day"] / 24
    pdf["hour_sin"] = np.sin(angle).astype("float32")
    pdf["hour_cos"] = np.cos(angle).astype("float32")

    return pdf


@dataclass(frozen=True)
class Dataset:
    """Un bloc du découpage, prêt pour l'entraînement ou l'évaluation."""

    name: str
    X: pd.DataFrame
    y: pd.Series
    context: pd.DataFrame

    @property
    def n_rows(self) -> int:
        return len(self.y)

    @property
    def n_frauds(self) -> int:
        return int(self.y.sum())

    @property
    def fraud_rate(self) -> float:
        return float(self.y.mean())

    def describe(self) -> str:
        return (
            f"{self.name:<12} {self.n_rows:>10,} lignes  "
            f"{self.n_frauds:>6,} fraudes  {100 * self.fraud_rate:>7.4f} %"
        ).replace(",", " ")


def split_datasets(pdf: pd.DataFrame) -> dict[str, Dataset]:
    """Découpe le tableau en trois blocs chronologiques indépendants."""
    datasets: dict[str, Dataset] = {}
    for name in ("train", "validation", "test"):
        block = pdf[pdf[SPLIT_COL] == name]
        datasets[name] = Dataset(
            name=name,
            X=block[list(ALL_FEATURES)],
            y=block[TARGET],
            context=block[list(CONTEXT_COLUMNS)],
        )
    return datasets


# ---------------------------------------------------------------------------
# Prétraitement
# ---------------------------------------------------------------------------
def build_preprocessor(scale: bool = False) -> ColumnTransformer:
    """Assemble le prétraitement commun aux quatre modèles comparés.

    **Valeurs manquantes.** XGBoost et LightGBM les traitent nativement, mais
    la régression logistique, la forêt aléatoire et SMOTE en sont incapables.
    Pour que la comparaison porte sur les algorithmes et non sur leur
    tolérance aux trous, tous reçoivent la même préparation : imputation par la
    médiane, **assortie d'une colonne indicatrice** (``add_indicator=True``).

    Cet indicateur est essentiel. Imputer seul reviendrait à effacer
    l'information « ce porteur n'avait pas d'historique » ; l'indicateur la
    conserve sous forme explicite. On ne déguise donc pas l'absence, on
    l'encode — ce qui reste conforme au principe posé en couche Gold.

    **Mise à l'échelle.** Indifférente aux modèles à arbres, indispensable à la
    régression logistique, dont la convergence et les coefficients dépendent de
    l'ordre de grandeur des variables. Elle n'est activée que pour elle.
    """
    numeric_steps: list = [
        ("imputer", SimpleImputer(strategy="median", add_indicator=True))
    ]
    if scale:
        numeric_steps.append(("scaler", StandardScaler()))

    return ColumnTransformer(
        transformers=[
            ("numeric", Pipeline(numeric_steps), list(NUMERIC_FEATURES)),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                list(CATEGORICAL_FEATURES),
            ),
        ],
        remainder="drop",
    )
