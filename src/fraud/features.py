"""Construction des variables comportementales (couche Gold).

C'est le cœur du projet, et la partie où une erreur invisible coûte le plus
cher.

## La contrainte qui gouverne tout ce module : pas de fuite temporelle

Une variable comme « montant moyen historique du client » se calcule sur une
fenêtre. Si cette fenêtre inclut la transaction courante, on injecte dans les
variables explicatives une information qui **n'existait pas au moment de la
décision**. Le modèle affiche alors des performances excellentes en évaluation
et s'effondre en production — et le défaut est indétectable en regardant les
métriques, puisqu'elles sont justement excellentes.

Toutes les fenêtres de ce module sont donc bornées à ``-1`` : strictement les
transactions **antérieures** du même client, jamais la courante. Le notebook
``03_gold_features`` en apporte la preuve formelle plutôt que l'affirmation.

## Deux familles de fenêtres, et pourquoi elles diffèrent

Spark propose deux bornages, qui ne répondent pas à la même question :

- ``rangeBetween`` borne sur la **valeur** de la colonne de tri. Avec un
  horodatage en secondes, ``rangeBetween(-3600, -1)`` sélectionne les
  transactions de l'heure écoulée, quel qu'en soit le nombre. C'est ce qu'il
  faut pour les compteurs de vélocité : la question est « combien dans la
  dernière heure ? », pas « combien parmi les N précédentes ? ».
- ``rowsBetween`` borne sur le **nombre de lignes**.
  ``rowsBetween(unboundedPreceding, -1)`` sélectionne tout l'historique
  antérieur du client. C'est ce qu'il faut pour établir son comportement de
  référence.

Contrainte technique : ``rangeBetween`` n'accepte **qu'une seule** colonne de
tri. Les fenêtres de vélocité trient donc sur le seul horodatage, tandis que
les fenêtres d'historique ajoutent l'identifiant de transaction comme critère
de départage. Sans ce départage, deux transactions du même client à la même
seconde seraient ordonnées arbitrairement et le résultat varierait d'une
exécution à l'autre.

## Valeurs manquantes : une absence n'est pas un zéro

La première transaction d'un client n'a pas d'historique. Deux traitements
coexistent, et la distinction est délibérée :

- Un **compteur** sur une fenêtre vide vaut ``0`` : « aucune transaction dans
  l'heure précédente » est une mesure, pas une absence d'information.
- Une **moyenne** sur une fenêtre vide vaut ``NULL`` : le montant habituel d'un
  client sans historique n'est pas nul, il est *indéterminé*.

Imputer ``0`` à la moyenne reviendrait à affirmer au modèle que ce client
dépense habituellement 0 $, ce qui est faux. XGBoost traite nativement les
valeurs manquantes en apprenant, pour chaque nœud, la direction par défaut la
plus favorable — il vaut donc mieux lui transmettre l'absence que la déguiser.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, WindowSpec
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Colonnes structurantes des fenêtres
# ---------------------------------------------------------------------------
ENTITY_COL = "customer_id"  # le comportement s'apprécie porteur par porteur
TIME_COL = "event_time_unix"  # tri temporel, en secondes (requis par rangeBetween)
TIEBREAK_COL = "transaction_id"  # départage déterministe à horodatage égal

# Largeurs des fenêtres de vélocité, en secondes.
# Ces valeurs ne sont pas conventionnelles : elles sont justifiées par la
# cadence réellement mesurée (note 01, constat 7). Un client normal transige
# toutes les 4,6 h en médiane, alors que 43,3 % des fraudes surviennent moins
# d'une heure après la précédente sur la même carte.
VELOCITY_WINDOWS: dict[str, int] = {
    "1h": 60 * 60,  # contraste maximal avec le rythme normal
    "24h": 24 * 60 * 60,  # couvre un cycle d'activité complet
    "7d": 7 * 24 * 60 * 60,  # référence de comportement récent
}

# Plage horaire nocturne, au sens du constat 4 : le taux de fraude y est 10 à
# 30 fois supérieur à celui de la journée.
NIGHT_HOURS = [22, 23, 0, 1, 2, 3]


# ---------------------------------------------------------------------------
# Définition des fenêtres
# ---------------------------------------------------------------------------
def velocity_window(seconds: int) -> WindowSpec:
    """Fenêtre glissante sur les ``seconds`` précédant la transaction courante.

    La borne haute est ``-1`` et non ``0`` : la transaction courante est exclue
    de sa propre fenêtre. Avec ``0``, un compteur se compterait lui-même et une
    somme inclurait le montant qu'on cherche justement à qualifier.

    Effet de bord assumé : les transactions d'un même client partageant la même
    seconde s'excluent mutuellement. C'est le choix conservateur — leur ordre
    réel est inconnaissable, donc aucune ne peut être déclarée antérieure.
    """
    return (
        Window.partitionBy(ENTITY_COL)
        .orderBy(F.col(TIME_COL).asc())
        .rangeBetween(-seconds, -1)
    )


def history_window() -> WindowSpec:
    """Fenêtre couvrant tout l'historique antérieur du client.

    Sert à établir son comportement de référence, sur lequel la transaction
    courante sera comparée.
    """
    return (
        Window.partitionBy(ENTITY_COL)
        .orderBy(F.col(TIME_COL).asc(), F.col(TIEBREAK_COL).asc())
        .rowsBetween(Window.unboundedPreceding, -1)
    )


def entity_history_window(*extra_keys: str) -> WindowSpec:
    """Historique antérieur du client, restreint à une combinaison de clés.

    Utilisée pour répondre à « ce client a-t-il déjà fréquenté ce commerçant ? »
    en partitionnant sur (client, commerçant).
    """
    return (
        Window.partitionBy(ENTITY_COL, *extra_keys)
        .orderBy(F.col(TIME_COL).asc(), F.col(TIEBREAK_COL).asc())
        .rowsBetween(Window.unboundedPreceding, -1)
    )


def ordinal_window() -> WindowSpec:
    """Fenêtre de rang, utilisée par le contrôle anti-fuite du notebook."""
    return Window.partitionBy(ENTITY_COL).orderBy(
        F.col(TIME_COL).asc(), F.col(TIEBREAK_COL).asc()
    )


# ---------------------------------------------------------------------------
# Blocs de variables
# ---------------------------------------------------------------------------
def add_velocity_features(df: DataFrame) -> DataFrame:
    """Compteurs et montants cumulés sur fenêtres glissantes.

    Ces variables capturent le scénario de fraude par rafales : une carte
    compromise est exploitée en série jusqu'à son blocage. Le contraste avec le
    rythme d'un porteur légitime est ce que le modèle doit apprendre.
    """
    for label, seconds in VELOCITY_WINDOWS.items():
        window = velocity_window(seconds)
        # Un compteur sur fenêtre vide vaut 0 : c'est une mesure, pas une absence.
        df = df.withColumn(f"txn_count_{label}", F.count("*").over(window))
        # Une somme sur fenêtre vide vaut NULL côté Spark ; ici 0 est le montant
        # réellement dépensé sur la période, donc la conversion est légitime.
        df = df.withColumn(
            f"amount_sum_{label}",
            F.coalesce(F.sum("amount").over(window), F.lit(0.0)),
        )

    # Délai depuis la transaction précédente. NULL sur la première transaction
    # d'un client : il n'y a pas de précédente, et zéro serait un mensonge.
    previous_time = F.lag(TIME_COL).over(ordinal_window())
    df = df.withColumn(
        "seconds_since_previous_txn", F.col(TIME_COL) - previous_time
    )
    return df


def add_behavioural_deviation_features(df: DataFrame) -> DataFrame:
    """Écart de la transaction courante au comportement habituel du client.

    C'est la famille de variables la plus directement liée à la problématique.
    Un seuil absolu (« au-delà de 500 $, c'est suspect ») produit
    mécaniquement des faux positifs chez les gros dépensiers et laisse passer
    les fraudes chez les petits. Une mesure **relative au client** ne souffre
    pas de ce défaut : elle répond à « inhabituel pour *lui* », qui est la
    question métier réelle.
    """
    history = history_window()

    df = (
        df.withColumn("customer_txn_count_hist", F.count("*").over(history))
        .withColumn("customer_amount_mean_hist", F.avg("amount").over(history))
        .withColumn("customer_amount_std_hist", F.stddev("amount").over(history))
        .withColumn("customer_amount_max_hist", F.max("amount").over(history))
    )

    # Écart-type nul (client aux montants identiques) ou indéfini (moins de deux
    # transactions) : le z-score n'a pas de sens, on le laisse manquant plutôt
    # que de produire une division par zéro ou une valeur infinie.
    std = F.col("customer_amount_std_hist")
    mean = F.col("customer_amount_mean_hist")

    df = df.withColumn(
        "amount_zscore_vs_history",
        F.when(std > 0, (F.col("amount") - mean) / std),
    ).withColumn(
        "amount_ratio_vs_mean",
        F.when(mean > 0, F.col("amount") / mean),
    )
    return df


def add_novelty_features(df: DataFrame) -> DataFrame:
    """Nouveauté du commerçant et de la catégorie pour ce client.

    Une carte compromise est typiquement utilisée chez des commerçants que son
    porteur n'a jamais fréquentés. La variable ne dit pas « ce commerçant est
    suspect » mais « ce client n'y était jamais allé » — une nuance qui évite de
    pénaliser injustement les petits commerçants.
    """
    merchant_history = entity_history_window("merchant_name")
    category_history = entity_history_window("merchant_category")

    df = (
        df.withColumn(
            "customer_merchant_count_hist", F.count("*").over(merchant_history)
        )
        .withColumn(
            "customer_category_count_hist", F.count("*").over(category_history)
        )
        .withColumn(
            "is_new_merchant_for_customer",
            F.col("customer_merchant_count_hist") == 0,
        )
        .withColumn(
            "is_new_category_for_customer",
            F.col("customer_category_count_hist") == 0,
        )
    )

    # Part de cette catégorie dans l'historique du client. Indéfinie tant qu'il
    # n'a pas d'historique — d'où le NULL plutôt qu'un zéro trompeur.
    total_history = F.col("customer_txn_count_hist")
    df = df.withColumn(
        "customer_category_share_hist",
        F.when(
            total_history > 0,
            F.col("customer_category_count_hist") / total_history,
        ),
    )
    return df


def add_temporal_features(df: DataFrame) -> DataFrame:
    """Variables de calendrier.

    L'heure de la journée est le signal le plus fort du corpus (constat 4) — et
    c'est en partie un artefact du générateur. Elle est conservée parce qu'elle
    correspond à un phénomène métier réel, mais son poids sera surveillé dans
    l'analyse SHAP : si elle écrase toutes les autres variables, c'est que le
    modèle apprend la règle de génération plutôt que le comportement de fraude.
    """
    hour = F.hour("event_time")
    return (
        df.withColumn("hour_of_day", hour)
        .withColumn("day_of_week", F.dayofweek("event_time"))
        .withColumn("is_weekend", F.dayofweek("event_time").isin([1, 7]))
        .withColumn("is_night", hour.isin(NIGHT_HOURS))
    )


# ---------------------------------------------------------------------------
# Assemblage
# ---------------------------------------------------------------------------
# Colonnes reprises telles quelles depuis Silver : identifiants, contexte et
# cible. Les identifiants ne sont pas des variables explicatives — ils servent
# à joindre, auditer et découper, et seront écartés à l'entraînement.
PASSTHROUGH_COLUMNS: tuple[str, ...] = (
    "transaction_id",
    "customer_id",
    "event_time",
    "event_date",
    "event_time_unix",
    "amount",
    "merchant_name",
    "merchant_category",
    "customer_age",
    "customer_gender",
    "customer_state",
    "customer_city_population",
    "is_fraud",
)

# Colonnes intermédiaires, utiles au calcul et au contrôle mais non destinées
# au modèle.
INTERNAL_COLUMNS: tuple[str, ...] = (
    "customer_merchant_count_hist",
    "customer_category_count_hist",
)


def build_gold_features(df: DataFrame) -> DataFrame:
    """Enchaîne les quatre familles de variables sur la table Silver.

    L'ordre importe : ``add_novelty_features`` réutilise
    ``customer_txn_count_hist`` produite par
    ``add_behavioural_deviation_features``.
    """
    df = add_velocity_features(df)
    df = add_behavioural_deviation_features(df)
    df = add_novelty_features(df)
    df = add_temporal_features(df)
    return df.withColumn("_gold_processed_at", F.current_timestamp())


# Variables explicatives soumises au modèle (étape 4).
# Sont volontairement exclus : les identifiants, l'horodatage brut et le nom du
# commerçant — ce dernier compte 693 modalités, dont l'encodage produirait un
# modèle qui mémorise des commerçants au lieu d'apprendre des comportements.
FEATURE_COLUMNS: tuple[str, ...] = (
    # Vélocité
    "txn_count_1h",
    "txn_count_24h",
    "txn_count_7d",
    "amount_sum_1h",
    "amount_sum_24h",
    "amount_sum_7d",
    "seconds_since_previous_txn",
    # Écart au comportement habituel
    "customer_txn_count_hist",
    "customer_amount_mean_hist",
    "customer_amount_std_hist",
    "customer_amount_max_hist",
    "amount_zscore_vs_history",
    "amount_ratio_vs_mean",
    # Nouveauté
    "is_new_merchant_for_customer",
    "is_new_category_for_customer",
    "customer_category_share_hist",
    # Temporel
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_night",
    # Contexte de la transaction
    "amount",
    "merchant_category",
    "customer_age",
    "customer_gender",
    "customer_state",
    "customer_city_population",
)


# ---------------------------------------------------------------------------
# Contrôles anti-fuite
# ---------------------------------------------------------------------------
def leakage_check_expressions() -> dict[str, Column]:
    """Expressions détectant une fuite temporelle dans les variables produites.

    Ces contrôles ne vérifient pas que le code *semble* correct : ils vérifient
    une propriété mathématique du résultat. C'est la différence entre affirmer
    l'absence de fuite et la démontrer.

    - ``history_count_mismatch`` : pour la k-ième transaction d'un client,
      ``customer_txn_count_hist`` doit valoir exactement k−1. Toute autre
      valeur signifie que la fenêtre ne contient pas précisément les lignes
      antérieures.
    - ``first_txn_has_history`` : la première transaction d'un client ne peut
      pas avoir de montant moyen historique.
    - ``velocity_not_monotonic`` : le nombre de transactions sur 1 h ne peut
      excéder celui sur 24 h, lui-même borné par celui sur 7 jours, lui-même
      borné par l'historique complet. Une inversion trahirait une fenêtre mal
      bornée.
    """
    ordinal = F.row_number().over(ordinal_window())
    hist_count = F.col("customer_txn_count_hist")

    return {
        "history_count_mismatch": hist_count != (ordinal - 1),
        "first_txn_has_history": (ordinal == 1)
        & F.col("customer_amount_mean_hist").isNotNull(),
        "velocity_not_monotonic": (F.col("txn_count_1h") > F.col("txn_count_24h"))
        | (F.col("txn_count_24h") > F.col("txn_count_7d"))
        | (F.col("txn_count_7d") > hist_count),
    }


# ---------------------------------------------------------------------------
# Dictionnaire de données — descriptions métier en français
# ---------------------------------------------------------------------------
GOLD_DATA_DICTIONARY: dict[str, dict[str, str]] = {
    "txn_count_1h": {
        "type": "BIGINT",
        "source": "fenêtre 1 h",
        "comment": "Nombre de transactions du porteur dans l'heure précédant celle-ci, transaction courante exclue. Largeur choisie sur mesure : 43,3 % des fraudes surviennent moins d'une heure après la précédente, contre un rythme normal d'une transaction toutes les 4,6 heures.",
    },
    "txn_count_24h": {
        "type": "BIGINT",
        "source": "fenêtre 24 h",
        "comment": "Nombre de transactions du porteur dans les 24 heures précédentes, transaction courante exclue. Couvre un cycle d'activité complet.",
    },
    "txn_count_7d": {
        "type": "BIGINT",
        "source": "fenêtre 7 j",
        "comment": "Nombre de transactions du porteur dans les 7 jours précédents, transaction courante exclue. Sert de référence d'activité récente.",
    },
    "amount_sum_1h": {
        "type": "DOUBLE",
        "source": "fenêtre 1 h",
        "comment": "Montant total dépensé par le porteur dans l'heure précédente. Vaut 0 en l'absence de transaction sur la période.",
    },
    "amount_sum_24h": {
        "type": "DOUBLE",
        "source": "fenêtre 24 h",
        "comment": "Montant total dépensé par le porteur dans les 24 heures précédentes. Vaut 0 en l'absence de transaction sur la période.",
    },
    "amount_sum_7d": {
        "type": "DOUBLE",
        "source": "fenêtre 7 j",
        "comment": "Montant total dépensé par le porteur dans les 7 jours précédents. Vaut 0 en l'absence de transaction sur la période.",
    },
    "seconds_since_previous_txn": {
        "type": "BIGINT",
        "source": "dérivée",
        "comment": "Délai en secondes depuis la transaction précédente du même porteur. Non renseigné pour sa première transaction.",
    },
    "customer_txn_count_hist": {
        "type": "BIGINT",
        "source": "historique complet",
        "comment": "Nombre total de transactions antérieures du porteur. Mesure la profondeur d'historique disponible : les variables comportementales sont peu fiables quand elle est faible.",
    },
    "customer_amount_mean_hist": {
        "type": "DOUBLE",
        "source": "historique complet",
        "comment": "Montant moyen des transactions antérieures du porteur. Non renseigné pour sa première transaction — un client sans historique n'a pas un montant habituel nul, il n'en a pas.",
    },
    "customer_amount_std_hist": {
        "type": "DOUBLE",
        "source": "historique complet",
        "comment": "Écart-type des montants antérieurs du porteur. Non renseigné en deçà de deux transactions.",
    },
    "customer_amount_max_hist": {
        "type": "DOUBLE",
        "source": "historique complet",
        "comment": "Montant le plus élevé jamais dépensé par le porteur avant cette transaction.",
    },
    "amount_zscore_vs_history": {
        "type": "DOUBLE",
        "source": "dérivée",
        "comment": "Écart du montant courant à la moyenne du porteur, exprimé en nombre d'écarts-types de son propre historique. Variable centrale du dispositif : elle mesure l'inhabituel relatif au client plutôt qu'un seuil absolu, qui pénaliserait les gros dépensiers et laisserait passer les fraudes chez les petits.",
    },
    "amount_ratio_vs_mean": {
        "type": "DOUBLE",
        "source": "dérivée",
        "comment": "Rapport entre le montant courant et le montant moyen historique du porteur. Lecture plus directe que le z-score, et robuste lorsque l'écart-type est très faible.",
    },
    "is_new_merchant_for_customer": {
        "type": "BOOLEAN",
        "source": "historique (client, commerçant)",
        "comment": "Vrai si le porteur n'a jamais effectué de transaction chez ce commerçant auparavant. Une carte compromise est typiquement utilisée chez des commerçants inconnus du porteur.",
    },
    "is_new_category_for_customer": {
        "type": "BOOLEAN",
        "source": "historique (client, catégorie)",
        "comment": "Vrai si le porteur n'a jamais effectué d'achat dans cette catégorie auparavant.",
    },
    "customer_category_share_hist": {
        "type": "DOUBLE",
        "source": "dérivée",
        "comment": "Part de cette catégorie dans l'historique d'achat du porteur. Une valeur proche de zéro signale une catégorie marginale pour lui.",
    },
    "hour_of_day": {
        "type": "INT",
        "source": "dérivée de event_time",
        "comment": "Heure de la transaction (0 à 23). Signal le plus discriminant du corpus : le taux de fraude est 10 à 30 fois supérieur entre 22 h et 3 h. Cette amplitude tient en partie à la règle de génération du jeu de données simulé, ce qui impose de surveiller son poids dans l'analyse d'explicabilité.",
    },
    "day_of_week": {
        "type": "INT",
        "source": "dérivée de event_time",
        "comment": "Jour de la semaine (1 = dimanche, 7 = samedi).",
    },
    "is_weekend": {
        "type": "BOOLEAN",
        "source": "dérivée de event_time",
        "comment": "Vrai si la transaction a lieu un samedi ou un dimanche.",
    },
    "is_night": {
        "type": "BOOLEAN",
        "source": "dérivée de event_time",
        "comment": "Vrai si la transaction a lieu entre 22 h et 3 h — plage où le taux de fraude observé est le plus élevé.",
    },
    "_gold_processed_at": {
        "type": "TIMESTAMP",
        "source": "dérivée",
        "comment": "Horodatage du calcul des variables comportementales (couche Gold).",
    },
}

GOLD_COLUMN_COMMENTS: dict[str, str] = {
    column: doc["comment"] for column, doc in GOLD_DATA_DICTIONARY.items()
}
