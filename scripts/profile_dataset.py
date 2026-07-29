"""Profilage exploratoire des fichiers sources Sparkov.

Produit tous les chiffres cités dans `docs/memoire/00_contexte_et_donnees.md`
et `docs/memoire/01_profilage_initial.md`. Le script est volontairement
autonome et sans dépendance Spark : il s'exécute en local sur les CSV bruts,
avant même la création du workspace Databricks.

Objectif méthodologique : aucune affirmation du mémoire ne doit reposer sur une
observation non rejouable. Ce script est la preuve exécutable des constats.

Usage :
    py -3.13 scripts/profile_dataset.py
    py -3.13 scripts/profile_dataset.py > docs/memoire/profiling_output.txt

Prérequis : pandas, numpy — et les CSV présents dans data/raw/ (voir data/README.md).
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
TRAIN_FILE = "fraudTrain.csv"
TEST_FILE = "fraudTest.csv"

EARTH_RADIUS_KM = 6371.0
SECONDS_PER_YEAR = 31_556_952  # année julienne moyenne


def rule(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def load(filename: str) -> pd.DataFrame:
    """Charge un CSV source et normalise l'horodatage de référence.

    `trans_date_trans_time` est l'horodatage métier faisant foi dans tout le
    projet (cf. constat 2 de la note 01 : `unix_time` en diverge de 7 ans).
    """
    df = pd.read_csv(RAW_DIR / filename, low_memory=False)
    df["event_time"] = pd.to_datetime(df["trans_date_trans_time"])
    return df


def haversine_km(
    lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series
) -> pd.Series:
    """Distance orthodromique en kilomètres entre deux points géographiques."""
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = (
        np.sin((lat2 - lat1) / 2) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


# ---------------------------------------------------------------------------
# Sections de profilage
# ---------------------------------------------------------------------------

def profile_volumetry(df: pd.DataFrame, label: str) -> None:
    """Volumétrie, période couverte et déséquilibre de classes."""
    rule(f"VOLUMETRIE — {label}")
    n_fraud = int(df["is_fraud"].sum())
    print(f"  Transactions          : {len(df):,}")
    print(f"  Colonnes              : {df.shape[1] - 1}")  # -1 : event_time ajoutée
    print(f"  Periode               : {df['event_time'].min()}  ->  {df['event_time'].max()}")
    print(f"  Fraudes               : {n_fraud:,}  ({100 * n_fraud / len(df):.4f} %)")
    print(f"  Ratio legitime:fraude : {(len(df) - n_fraud) / n_fraud:.0f} : 1")
    print(f"  Porteurs distincts    : {df['cc_num'].nunique()}")
    print(f"  Commercants distincts : {df['merchant'].nunique()}")
    print(f"  Categories            : {df['category'].nunique()}")


def profile_quality(df: pd.DataFrame) -> None:
    """Constat 1 — contrôles de qualité brute (note 01, § 2)."""
    rule("CONSTAT 1 — QUALITE BRUTE")
    checks = {
        "Valeurs manquantes (total)": int(df.isna().sum().sum()),
        "trans_num dupliques": int(df["trans_num"].duplicated().sum()),
        "Lignes strictement identiques": int(df.duplicated().sum()),
        "Montants <= 0": int((df["amt"] <= 0).sum()),
        "Horodatages invalides": int(df["event_time"].isna().sum()),
    }
    for name, value in checks.items():
        status = "OK " if value == 0 else "!! "
        print(f"  {status} {name:<32} {value:>10,}")


def profile_unix_time_consistency(df: pd.DataFrame) -> None:
    """Constat 2 — cohérence croisée entre les deux colonnes temporelles.

    Contrôle non trivial : chacune des deux colonnes est plausible prise
    isolément. Seule leur confrontation révèle le décalage systématique.
    """
    rule("CONSTAT 2 — COHERENCE unix_time vs trans_date_trans_time")
    from_unix = pd.to_datetime(df["unix_time"], unit="s", utc=True).dt.tz_localize(None)
    delta = (df["event_time"] - from_unix).dt.total_seconds()
    print(f"  Ecart minimal              : {delta.min():>15,.0f} s")
    print(f"  Ecart maximal              : {delta.max():>15,.0f} s")
    print(f"  Valeurs d'ecart distinctes : {delta.nunique():>15}")
    print(f"  Ecart moyen en annees      : {delta.mean() / SECONDS_PER_YEAR:>15.4f}")
    print(f"  Exemple : {df['event_time'].iloc[0]}  vs  {from_unix.iloc[0]} (depuis unix_time)")
    print("  -> decalage systematique du generateur, pas une corruption")


def profile_merchant_prefix(df: pd.DataFrame) -> None:
    """Constat 3 — le préfixe `fraud_` est-il corrélé à la cible ?"""
    rule("CONSTAT 3 — PREFIXE 'fraud_' DES COMMERCANTS")
    prefixed = df["merchant"].str.startswith("fraud_")
    print(f"  Transactions prefixees : {prefixed.sum():,} / {len(df):,} ({100 * prefixed.mean():.1f} %)")
    if prefixed.all():
        print("  -> prefixe applique a 100 % des commercants : artefact de nommage,")
        print("     AUCUNE fuite de la variable cible. Retire en couche Silver.")


def profile_hour_of_day(df: pd.DataFrame) -> None:
    """Constat 4 — distribution horaire de la fraude."""
    rule("CONSTAT 4 — TAUX DE FRAUDE PAR HEURE")
    rates = df.groupby(df["event_time"].dt.hour)["is_fraud"].mean() * 100
    for hour in range(24):
        rate = rates.get(hour, 0.0)
        print(f"    {hour:02d}h  {rate:6.3f} %  {'#' * int(rate * 4)}")
    night = rates[[22, 23, 0, 1, 2, 3]]
    day = rates[[h for h in range(4, 22)]]
    print(f"  Plage nocturne (22h-03h) : {night.min():.3f} % - {night.max():.3f} %")
    print(f"  Plage diurne   (04h-21h) : {day.min():.3f} % - {day.max():.3f} %")
    print(f"  Rapport nocturne/diurne  : x{night.min() / day.max():.0f} a x{night.max() / day.min():.0f}")


def profile_geographic_distance(df: pd.DataFrame) -> None:
    """Constat 5 — la distance porteur/commerçant discrimine-t-elle ?

    Testée AVANT d'être retenue comme variable : c'est ce test qui conduit à
    l'écarter du jeu de features (résultat négatif documenté).
    """
    rule("CONSTAT 5 — DISTANCE PORTEUR <-> COMMERCANT (km)")
    dist = haversine_km(df["lat"], df["long"], df["merch_lat"], df["merch_long"])
    stats = df.assign(distance_km=dist).groupby("is_fraud")["distance_km"].agg(
        ["mean", "median", "max"]
    )
    print(stats.to_string())
    gap = abs(stats.loc[1, "mean"] - stats.loc[0, "mean"])
    print(f"  Ecart des moyennes entre classes : {gap:.2f} km")
    print("  -> distributions indiscernables : variable ECARTEE du jeu de features")


def profile_amount(df: pd.DataFrame) -> None:
    """Constat 6 — pouvoir discriminant du montant et plafonnement artificiel."""
    rule("CONSTAT 6 — MONTANT PAR CLASSE ($)")
    stats = df.groupby("is_fraud")["amt"].agg(["count", "mean", "median", "max"])
    print(stats.to_string())
    print(f"  Rapport des medianes (fraude/legitime) : x{stats.loc[1, 'median'] / stats.loc[0, 'median']:.1f}")
    print(f"  Montant frauduleux maximal : {stats.loc[1, 'max']:,.2f} $")
    print(f"  Montant legitime  maximal  : {stats.loc[0, 'max']:,.2f} $")
    print("  -> plafond artificiel sur les fraudes : un montant tres eleve")
    print("     est ici un indicateur de transaction LEGITIME")


def profile_velocity(df: pd.DataFrame) -> None:
    """Constat 7 — cadence normale vs cadence de fraude.

    Fournit la justification chiffrée des largeurs de fenêtre de l'étape 3.
    """
    rule("CONSTAT 7 — CADENCE DES TRANSACTIONS (base des features de velocite)")
    ordered = df.sort_values(["cc_num", "event_time"])

    normal_gap = ordered.groupby("cc_num")["event_time"].diff().dt.total_seconds()
    print("  Client normal :")
    print(f"    Transactions par client (moyenne) : {len(df) / df['cc_num'].nunique():.0f}")
    print(f"    Delai median entre 2 transactions : {normal_gap.median() / 3600:.2f} h")
    print(f"    Percentile 05 / 95                : {normal_gap.quantile(0.05) / 60:.1f} min / {normal_gap.quantile(0.95) / 3600:.1f} h")

    frauds = ordered[ordered["is_fraud"] == 1]
    fraud_gap = frauds.groupby("cc_num")["event_time"].diff().dt.total_seconds()
    print("  Episode de fraude (meme carte) :")
    print(f"    Delai median entre 2 fraudes      : {fraud_gap.median() / 60:.1f} min")
    print(f"    Fraudes survenant < 1 h apres la precedente : {100 * (fraud_gap < 3600).mean():.1f} %")
    print(f"  -> contraste x{(normal_gap.median() / fraud_gap.median()):.1f} : la velocite est le bon axe de features")


def profile_cardholder_age(df: pd.DataFrame) -> None:
    """Contrôle de plausibilité métier sur l'âge du porteur."""
    rule("CONTROLE DE PLAUSIBILITE — AGE DU PORTEUR")
    age = (df["event_time"] - pd.to_datetime(df["dob"])).dt.days / 365.25
    print(df.assign(age=age).groupby("is_fraud")["age"].agg(["mean", "median"]).to_string())
    minors = int((age < 18).sum())
    print(f"  Age min / max : {age.min():.1f} / {age.max():.1f}")
    print(f"  Transactions avec porteur de moins de 18 ans : {minors:,} ({100 * minors / len(df):.2f} %)")
    print("  -> SIGNALE en Silver (indicateur), mais NON rejete : un mineur peut")
    print("     legitimement etre porteur secondaire d'une carte")


def profile_temporal_split(train: pd.DataFrame, test: pd.DataFrame) -> None:
    """Vérifie que la séparation train/test fournie est bien temporelle."""
    rule("PROTOCOLE — SEPARATION TEMPORELLE TRAIN / TEST")
    print(f"  Train : {train['event_time'].min()}  ->  {train['event_time'].max()}")
    print(f"  Test  : {test['event_time'].min()}  ->  {test['event_time'].max()}")
    overlap = train["event_time"].max() >= test["event_time"].min()
    print(f"  Chevauchement temporel : {'OUI (probleme)' if overlap else 'NON (conforme)'}")
    common = set(train["cc_num"]) & set(test["cc_num"])
    union = set(train["cc_num"]) | set(test["cc_num"])
    print(f"  Porteurs : train={train['cc_num'].nunique()}  test={test['cc_num'].nunique()}"
          f"  communs={len(common)}  union={len(union)}")
    print(f"  Taux de fraude : train={100 * train['is_fraud'].mean():.4f} %"
          f"  test={100 * test['is_fraud'].mean():.4f} %  (derive a documenter)")


def main() -> None:
    # Sous Windows, la console utilise cp1252 : une redirection vers un fichier
    # corromprait les caractères non-ASCII. On force UTF-8 pour que la sortie
    # archivée dans docs/memoire/ soit lisible.
    with contextlib.suppress(AttributeError):
        sys.stdout.reconfigure(encoding="utf-8")

    train = load(TRAIN_FILE)
    test = load(TEST_FILE)
    combined = pd.concat([train, test], ignore_index=True)

    profile_volumetry(train, TRAIN_FILE)
    profile_volumetry(test, TEST_FILE)
    profile_volumetry(combined, "CORPUS COMPLET")

    # Les constats se mesurent sur le jeu d'entraînement : c'est lui qui
    # détermine les règles du pipeline, le test devant rester intouché.
    profile_quality(combined)
    profile_unix_time_consistency(train)
    profile_merchant_prefix(train)
    profile_hour_of_day(train)
    profile_geographic_distance(train)
    profile_amount(train)
    profile_velocity(train)
    profile_cardholder_age(train)

    profile_temporal_split(train, test)


if __name__ == "__main__":
    main()
