"""Configuration centrale du pipeline de détection de fraude.

Toute la nomenclature Unity Catalog (catalogue, schéma, volume, tables) est
définie ici et nulle part ailleurs. Deux raisons :

1. **Maintenabilité** : si le workspace Databricks change (autre catalogue,
   autre schéma), il y a un seul fichier à modifier au lieu de cinq notebooks.
2. **Lisibilité** : les notebooks manipulent `config.BRONZE_TABLE` plutôt que
   de répéter la chaîne `workspace.fraud_detection.bronze_transactions`, ce qui
   supprime une classe entière de bugs (fautes de frappe sur un nom de table).

Les valeurs sont surchargeables par variable d'environnement, ce qui permet de
faire tourner le même code sur un workspace de test sans toucher au code.
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Unity Catalog
# ---------------------------------------------------------------------------
# Sur Databricks Free Edition, le catalogue par défaut du workspace s'appelle
# `workspace`. On y crée un schéma dédié au projet plutôt que d'écrire dans
# `default` : un projet = un schéma, c'est la convention Databricks.
CATALOG: str = os.getenv("FRAUD_CATALOG", "workspace")
SCHEMA: str = os.getenv("FRAUD_SCHEMA", "fraud_detection")

# Volume Unity Catalog qui héberge les CSV sources.
# Un *volume* est l'emplacement de stockage géré par Unity Catalog pour les
# fichiers non tabulaires. On l'utilise à la place de DBFS, déprécié.
VOLUME: str = os.getenv("FRAUD_VOLUME", "raw")


def fqn(table: str) -> str:
    """Retourne le nom pleinement qualifié d'une table (`catalog.schema.table`)."""
    return f"{CATALOG}.{SCHEMA}.{table}"


# ---------------------------------------------------------------------------
# Tables de l'architecture Medallion
# ---------------------------------------------------------------------------
BRONZE_TABLE: str = fqn("bronze_transactions")  # brut, fidèle à la source
SILVER_TABLE: str = fqn("silver_transactions")  # typé, nettoyé, dédupliqué
GOLD_FEATURES_TABLE: str = fqn("gold_features")  # variables comportementales
GOLD_SCORED_TABLE: str = fqn("gold_scored_transactions")  # + score et décision

# Table technique : journal des rejets de la qualité de données (étape 2).
# On ne jette JAMAIS une ligne invalide silencieusement — on la trace, on la
# compte, et on l'explique dans le mémoire.
QUARANTINE_TABLE: str = fqn("silver_quarantine")

# ---------------------------------------------------------------------------
# Streaming (étape 8)
# ---------------------------------------------------------------------------
# Répertoire d'atterrissage : c'est là qu'un système amont déposerait ses
# fichiers au fil de l'eau. Auto Loader le surveille et n'ingère que les
# nouveautés.
LANDING_VOLUME: str = os.getenv("FRAUD_LANDING_VOLUME", "landing")

# Les points de reprise sont ce qui rend un flux redémarrable. Ils mémorisent
# les fichiers déjà traités et la position atteinte : sans eux, un
# redémarrage réingérerait tout depuis le début.
CHECKPOINT_VOLUME: str = os.getenv("FRAUD_CHECKPOINT_VOLUME", "checkpoints")

# Tables produites par la chaîne en flux. Elles sont DISTINCTES des tables
# batch, volontairement : les deux chaînes doivent pouvoir coexister et être
# comparées. Écraser les tables batch interdirait toute vérification que le
# streaming produit bien le même résultat.
BRONZE_STREAM_TABLE: str = fqn("bronze_transactions_stream")
SILVER_STREAM_TABLE: str = fqn("silver_transactions_stream")
SCORED_STREAM_TABLE: str = fqn("gold_scored_stream")

# Profil comportemental par porteur, calculé en batch et consommé par le flux.
# C'est la clé de voûte de l'architecture streaming (cf. note 11) : les
# agrégats d'historique ne peuvent pas être recalculés à chaque transaction.
CUSTOMER_PROFILE_TABLE: str = fqn("gold_customer_profile")

# ---------------------------------------------------------------------------
# Chemins des fichiers sources dans le volume
# ---------------------------------------------------------------------------
VOLUME_PATH: str = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_TRAIN_PATH: str = f"{VOLUME_PATH}/fraudTrain.csv"
RAW_TEST_PATH: str = f"{VOLUME_PATH}/fraudTest.csv"

LANDING_PATH: str = f"/Volumes/{CATALOG}/{SCHEMA}/{LANDING_VOLUME}"
CHECKPOINT_PATH: str = f"/Volumes/{CATALOG}/{SCHEMA}/{CHECKPOINT_VOLUME}"

# ---------------------------------------------------------------------------
# Colonnes clés du dataset Sparkov
# ---------------------------------------------------------------------------
# Nommées ici pour éviter les littéraux dispersés : si la source change de
# nom de colonne, un seul endroit à corriger.
TARGET_COL: str = "is_fraud"  # variable cible (1 = fraude)
EVENT_TIME_COL: str = "trans_date_trans_time"  # horodatage métier
ENTITY_COL: str = "cc_num"  # identifiant client (numéro de carte)
AMOUNT_COL: str = "amt"  # montant de la transaction
TRANSACTION_ID_COL: str = "trans_num"  # identifiant unique de transaction

# ---------------------------------------------------------------------------
# Découpage temporel (étape 4)
# ---------------------------------------------------------------------------
# En détection de fraude, l'évaluation doit répondre à « le modèle entraîné sur
# le passé détecte-t-il la fraude de demain ? ». Un tirage aléatoire
# entraînerait le modèle sur des transactions postérieures à celles du test —
# situation impossible en production, qui surestime les performances.
#
# Le découpage est donc chronologique et à TROIS blocs, et non deux :
#
#   entraînement  → ajustement des modèles
#   validation    → comparaison des modèles et choix du seuil de décision
#   test          → évaluation finale, jamais consultée avant la fin
#
# Le bloc de validation n'est pas un raffinement académique. Choisir le seuil
# de décision directement sur le jeu de test reviendrait à l'optimiser sur les
# données censées mesurer la performance : le taux de faux positifs annoncé
# serait alors optimiste, et c'est précisément le chiffre central du mémoire.
TRAIN_END: str = "2020-04-01 00:00:00"  # 15 mois d'entraînement
VALIDATION_END: str = "2020-06-21 12:14:25"  # frontière native du corpus Kaggle

# ---------------------------------------------------------------------------
# Pseudonymisation (couche Silver)
# ---------------------------------------------------------------------------
# Sel utilisé pour le hachage du numéro de carte.
#
# Sans sel, le haché d'un numéro de carte est reconstituable par force brute :
# l'espace des numéros valides est contraint (préfixes émetteurs, longueur fixe,
# clé de Luhn), et quiconque disposerait de la table pourrait retrouver les
# numéros d'origine. Le hachage seul ne constitue donc pas une pseudonymisation
# au sens de l'article 4(5) du RGPD.
#
# LIMITE ASSUMÉE : ce sel figure en clair dans le dépôt. C'est acceptable ici
# car le jeu de données est intégralement synthétique — aucun numéro de carte
# réel n'est concerné. Sur des données de production, il devrait provenir d'un
# secret scope Databricks (`dbutils.secrets.get`), faute de quoi la
# pseudonymisation serait réversible par toute personne ayant accès au code.
# La variable d'environnement permet précisément cette substitution sans
# modifier une ligne du pipeline.
PSEUDONYM_SALT: str = os.getenv("FRAUD_PSEUDONYM_SALT", "mia26-fraud-detection")

# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------
MLFLOW_EXPERIMENT: str = os.getenv(
    "FRAUD_MLFLOW_EXPERIMENT", "/Shared/fraud_detection"
)
REGISTERED_MODEL_NAME: str = fqn("fraud_detection_model")

# Alias désignant la version en service. Le notebook de scoring charge
# `models:/<modèle>@champion` plutôt qu'un numéro figé : promouvoir un nouveau
# modèle revient alors à déplacer l'alias, sans toucher au code en aval.
MODEL_ALIAS: str = "champion"

# Configuration retenue à l'issue de la comparaison (étape 4). Déclarée ici
# pour que le notebook de scoring puisse VÉRIFIER qu'il charge bien le modèle
# attendu : le registre contient aussi les candidats écartés, et rien
# n'empêcherait de scorer avec l'un d'eux sans s'en apercevoir.
FINAL_MODEL_NAME: str = "xgb_sans_reequilibrage"

# ---------------------------------------------------------------------------
# Modèle de coût métier (étape 5)
# ---------------------------------------------------------------------------
# Le seuil de décision ne se choisit pas sur une métrique statistique mais sur
# un arbitrage économique. Les deux erreurs n'ont ni la même nature ni le même
# coût :
#
#   FAUX NÉGATIF  — une fraude non détectée. Le coût est le montant de la
#                   transaction, disponible dans les données : il n'a pas à
#                   être supposé.
#   FAUX POSITIF  — une transaction légitime bloquée. Le coût est fixe et
#                   composite : temps d'analyste, contact client, friction
#                   commerciale. Il doit être posé en hypothèse.
#
# La valeur ci-dessous est une hypothèse de travail, pas une mesure. Le
# notebook conduit donc une analyse de sensibilité sur plusieurs valeurs
# plutôt que de faire reposer la conclusion sur un seul chiffre arbitraire.
FALSE_ALERT_COST: float = float(os.getenv("FRAUD_FALSE_ALERT_COST", "5.0"))
FALSE_ALERT_COST_SCENARIOS: tuple[float, ...] = (2.0, 5.0, 10.0, 25.0)


def describe() -> str:
    """Résumé lisible de la configuration active — à afficher en tête de notebook.

    Sert de trace d'exécution : dans un rendu de notebook joint au mémoire, on
    voit immédiatement sur quel catalogue et quelles tables le run a travaillé.
    """
    return "\n".join(
        [
            "Configuration du pipeline",
            "-" * 40,
            f"  Catalogue        : {CATALOG}",
            f"  Schéma           : {SCHEMA}",
            f"  Volume (sources) : {VOLUME_PATH}",
            "",
            f"  Bronze           : {BRONZE_TABLE}",
            f"  Silver           : {SILVER_TABLE}",
            f"  Quarantaine      : {QUARANTINE_TABLE}",
            f"  Gold (features)  : {GOLD_FEATURES_TABLE}",
            f"  Gold (scorée)    : {GOLD_SCORED_TABLE}",
            "",
            f"  Expérience MLflow: {MLFLOW_EXPERIMENT}",
            f"  Modèle enregistré: {REGISTERED_MODEL_NAME}",
        ]
    )
