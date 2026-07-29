"""Transformation Bronze → Silver : typage, conformité et pseudonymisation.

La couche Silver a trois responsabilités, et une seule d'entre elles correspond
à ce qu'on appelle habituellement du « nettoyage » :

1. **Établir le contrat de données** — Bronze ne contient que du texte. C'est
   ici que chaque colonne reçoit son type définitif, et qu'une valeur qui ne s'y
   conforme pas devient une erreur explicite plutôt qu'un ``NULL`` silencieux.
2. **Contrôler et tracer la qualité** — les règles sont exécutées, comptées, et
   leur résultat publié. Le profilage (note 01) a montré que le corpus ne
   contient aucune valeur manquante ni aucun doublon : les contrôles ne
   rejetteront donc rien. Ils ne sont pas inutiles pour autant — ils *prouvent*
   la conformité au lieu de la supposer, et ils protégeront le pipeline le jour
   où la source changera.
3. **Assurer la conformité réglementaire** — pseudonymisation du porteur et
   suppression des identifiants directs, par transposition des exigences RGPD.

Une réparation réelle y est en revanche appliquée : la restauration des zéros de
tête des codes postaux, détruits en amont par un traitement numérique
(cf. ``docs/memoire/03_qualite_silver.md``).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from fraud.quality import QualityRule, Severity

# ---------------------------------------------------------------------------
# Colonnes techniques internes, préfixées `_v` (validation).
# Elles servent aux règles de qualité puis sont écartées de la table finale.
# ---------------------------------------------------------------------------
PARSED_EVENT_TIME = "_v_event_time"
PARSED_AMOUNT = "_v_amount"
PARSED_AGE = "_v_customer_age"
DUPLICATE_RANK = "_v_duplicate_rank"

# Colonnes de Bronze supprimées en Silver, et pourquoi.
DROPPED_COLUMNS: dict[str, str] = {
    "source_row_index": "index technique de l'export CSV, sans valeur métier",
    "first": "identifiant direct — prénom du porteur (RGPD)",
    "last": "identifiant direct — nom du porteur (RGPD)",
    "street": "identifiant direct — adresse postale précise (RGPD)",
    "cc_num": "remplacé par customer_id, pseudonymisé",
    "dob": "donnée personnelle — remplacée par customer_age, non identifiant",
    "unix_time": "incohérent avec l'horodatage (décalage de 7 ans) — recalculé",
}


def add_validation_helpers(df: DataFrame) -> DataFrame:
    """Ajoute les colonnes dérivées nécessaires aux règles de qualité.

    Ces colonnes sont calculées **avant** l'application des règles, car les
    règles portent précisément sur le résultat de la conversion : une valeur
    d'origine non nulle qui produit un ``NULL`` après conversion est un échec de
    parsing, et c'est exactement ce qu'il faut détecter.

    L'ordre de déduplication est totalement déterministe (fichier source, puis
    index de ligne). Sans tri stable, deux exécutions pourraient conserver des
    exemplaires différents d'un même doublon, et le pipeline cesserait d'être
    reproductible.
    """
    dedup_window = Window.partitionBy("trans_num").orderBy(
        F.col("_source_file").asc(), F.col("source_row_index").cast("long").asc()
    )

    return (
        df.withColumn(PARSED_EVENT_TIME, F.to_timestamp("trans_date_trans_time"))
        .withColumn(PARSED_AMOUNT, F.col("amt").cast("double"))
        .withColumn(
            PARSED_AGE,
            F.floor(
                F.months_between(F.col(PARSED_EVENT_TIME), F.to_date("dob")) / F.lit(12)
            ).cast("int"),
        )
        .withColumn(DUPLICATE_RANK, F.row_number().over(dedup_window))
    )


def _is_blank(column: str) -> Column:
    """Vrai si la colonne est nulle ou ne contient que des espaces."""
    return F.col(column).isNull() | (F.length(F.trim(F.col(column))) == 0)


def _fails_to_parse(source: str, parsed: str) -> Column:
    """Vrai si une valeur d'origine renseignée n'a pas pu être convertie."""
    return ~_is_blank(source) & F.col(parsed).isNull()


def reject_rules() -> list[QualityRule]:
    """Règles dont l'infraction envoie la ligne en quarantaine.

    Toutes portent sur l'**exploitabilité structurelle** de la ligne. Aucune ne
    porte de jugement métier : une transaction bizarre reste une transaction, et
    c'est souvent celle-là qui est frauduleuse. Écarter les valeurs atypiques
    au nom du nettoyage reviendrait à supprimer le signal que le modèle doit
    apprendre.
    """
    return [
        QualityRule(
            name="transaction_id_missing",
            description="Identifiant de transaction absent ou vide — la ligne n'est pas traçable.",
            severity=Severity.REJECT,
            violated_when=_is_blank("trans_num"),
        ),
        QualityRule(
            name="transaction_id_duplicated",
            description="Identifiant de transaction déjà présent — exemplaire surnuméraire.",
            severity=Severity.REJECT,
            violated_when=F.col(DUPLICATE_RANK) > 1,
        ),
        QualityRule(
            name="event_time_unparseable",
            description="Horodatage illisible — sans date, ni fenêtre temporelle ni split possible.",
            severity=Severity.REJECT,
            violated_when=_fails_to_parse("trans_date_trans_time", PARSED_EVENT_TIME),
        ),
        QualityRule(
            name="amount_unparseable",
            description="Montant non numérique.",
            severity=Severity.REJECT,
            violated_when=_fails_to_parse("amt", PARSED_AMOUNT),
        ),
        QualityRule(
            name="amount_not_positive",
            description="Montant négatif ou nul — un achat par carte porte un montant strictement positif.",
            severity=Severity.REJECT,
            violated_when=F.col(PARSED_AMOUNT) <= 0,
        ),
        QualityRule(
            name="customer_key_missing",
            description="Numéro de carte absent — la ligne ne peut être rattachée à un porteur.",
            severity=Severity.REJECT,
            violated_when=_is_blank("cc_num"),
        ),
        QualityRule(
            name="target_out_of_domain",
            description="Variable cible hors du domaine {0, 1} — étiquette inexploitable.",
            severity=Severity.REJECT,
            violated_when=~F.col("is_fraud").isin("0", "1"),
        ),
        QualityRule(
            name="customer_coordinates_invalid",
            description="Coordonnées du porteur hors bornes géographiques valides.",
            severity=Severity.REJECT,
            violated_when=~F.col("lat").cast("double").between(-90, 90)
            | ~F.col("long").cast("double").between(-180, 180),
        ),
        QualityRule(
            name="merchant_coordinates_invalid",
            description="Coordonnées du commerçant hors bornes géographiques valides.",
            severity=Severity.REJECT,
            violated_when=~F.col("merch_lat").cast("double").between(-90, 90)
            | ~F.col("merch_long").cast("double").between(-180, 180),
        ),
    ]


def flag_rules() -> list[QualityRule]:
    """Règles dont l'infraction marque la ligne sans l'écarter.

    Ce sont des anomalies **métier**, pas techniques. La ligne reste
    parfaitement exploitable ; c'est à l'analyse, et non à la couche technique,
    de décider quoi en faire.
    """
    return [
        QualityRule(
            name="flag_minor_cardholder",
            description="Porteur âgé de moins de 18 ans au moment de la transaction — "
            "invraisemblable pour un titulaire principal, plausible pour un porteur secondaire.",
            severity=Severity.FLAG,
            violated_when=F.col(PARSED_AGE) < 18,
        ),
        QualityRule(
            name="flag_zip_code_repaired",
            description="Code postal reçu sur 4 caractères — zéro de tête restauré par le pipeline.",
            severity=Severity.FLAG,
            violated_when=F.length(F.trim(F.col("zip"))) == 4,
        ),
    ]


def pseudonymize(column: str, salt: str) -> Column:
    """Pseudonymise un identifiant par hachage SHA-256 salé.

    **Pourquoi un sel est indispensable.** Sans sel, le haché d'un numéro de
    carte est reconstituable par force brute : l'espace des numéros valides est
    contraint (préfixes émetteurs connus, longueur fixe, clé de Luhn), et un
    attaquant disposant de la table pourrait retrouver les numéros d'origine en
    testant les candidats. Le hachage seul n'est donc **pas** une
    pseudonymisation au sens de l'article 4(5) du RGPD ; avec un sel tenu
    séparément, il en devient une.

    **Pourquoi le hachage plutôt qu'un identifiant séquentiel.** Le haché est
    déterministe : une même carte produit toujours le même identifiant, y
    compris entre deux exécutions et entre les lots train et test. Cette
    stabilité est indispensable — sans elle, aucune variable comportementale ne
    pourrait être calculée sur l'historique d'un porteur.
    """
    return F.sha2(F.concat(F.lit(salt), F.col(column)), 256)


def build_silver_projection(df: DataFrame, salt: str) -> DataFrame:
    """Construit la table Silver finale à partir des lignes déjà validées.

    Cette projection s'applique **après** le filtrage qualité : toutes les
    conversions y sont donc sûres, puisque leur convertibilité a été contrôlée
    en amont.
    """
    return df.select(
        # --- Identification et temps -------------------------------------
        F.col("trans_num").alias("transaction_id"),
        F.col(PARSED_EVENT_TIME).alias("event_time"),
        F.to_date(F.col(PARSED_EVENT_TIME)).alias("event_date"),
        # Horodatage en secondes, recalculé depuis l'horodatage de référence.
        # Il remplace la colonne `unix_time` de la source, décalée de 7 ans, et
        # sera requis par les fenêtres glissantes de la couche Gold : Spark ne
        # sait borner un `rangeBetween` que sur une valeur numérique.
        F.unix_timestamp(F.col(PARSED_EVENT_TIME)).alias("event_time_unix"),
        # --- Porteur (pseudonymisé) --------------------------------------
        pseudonymize("cc_num", salt).alias("customer_id"),
        F.col("gender").alias("customer_gender"),
        F.col(PARSED_AGE).alias("customer_age"),
        F.col("job").alias("customer_job"),
        F.col("city").alias("customer_city"),
        F.col("state").alias("customer_state"),
        # Restauration du zéro de tête détruit en amont (cf. note 03).
        F.lpad(F.trim(F.col("zip")), 5, "0").alias("customer_zip"),
        F.col("city_pop").cast("int").alias("customer_city_population"),
        F.col("lat").cast("double").alias("customer_lat"),
        F.col("long").cast("double").alias("customer_lon"),
        # --- Commerçant ---------------------------------------------------
        # Le préfixe `fraud_` est présent sur 100 % des commerçants : c'est un
        # artefact de nommage du générateur, vérifié comme non corrélé à la
        # cible (note 01, constat 3). On le retire pour lisibilité.
        F.regexp_replace(F.col("merchant"), "^fraud_", "").alias("merchant_name"),
        F.col("category").alias("merchant_category"),
        F.col("merch_lat").cast("double").alias("merchant_lat"),
        F.col("merch_long").cast("double").alias("merchant_lon"),
        # --- Transaction et cible ----------------------------------------
        F.col(PARSED_AMOUNT).alias("amount"),
        F.col("is_fraud").cast("int").alias("is_fraud"),
        # --- Indicateurs de qualité --------------------------------------
        F.col("flag_minor_cardholder"),
        F.col("flag_zip_code_repaired"),
        # --- Traçabilité --------------------------------------------------
        F.col("_ingested_at"),
        F.col("_source_file"),
        F.col("_ingestion_id"),
        F.current_timestamp().alias("_silver_processed_at"),
    )


# ---------------------------------------------------------------------------
# Dictionnaire de données — descriptions métier en français
# ---------------------------------------------------------------------------
# Ces commentaires sont posés sur les colonnes d'Unity Catalog et s'affichent
# dans le Catalog Explorer. Les noms techniques restent en anglais (standard
# professionnel, cohérent avec la source Kaggle) tandis que la documentation
# fonctionnelle est en français, langue du mémoire et de la soutenance.
#
# Source unique de vérité : ce dictionnaire alimente à la fois les commentaires
# posés dans Unity Catalog (notebook 02) et le document
# `docs/dictionnaire_donnees.md` (généré par `scripts/generate_data_dictionary.py`).
# Documenter à deux endroits garantirait une divergence ; on documente ici.
SILVER_DATA_DICTIONARY: dict[str, dict[str, str]] = {
    "transaction_id": {
        "type": "STRING",
        "source": "trans_num",
        "comment": "Identifiant unique de la transaction (empreinte de 32 caractères issue de la source).",
    },
    "event_time": {
        "type": "TIMESTAMP",
        "source": "trans_date_trans_time",
        "comment": "Horodatage de la transaction. Référence temporelle unique du pipeline : toutes les fenêtres, agrégations et découpages train/test s'appuient sur cette colonne.",
    },
    "event_date": {
        "type": "DATE",
        "source": "dérivée de event_time",
        "comment": "Date de la transaction. Facilite les agrégations quotidiennes du tableau de bord.",
    },
    "event_time_unix": {
        "type": "BIGINT",
        "source": "dérivée de event_time",
        "comment": "Horodatage en secondes depuis le 1er janvier 1970. Requis par les fenêtres glissantes de la couche Gold, Spark ne sachant borner un rangeBetween que sur une valeur numérique. Remplace la colonne unix_time de la source, décalée de 7 ans.",
    },
    "customer_id": {
        "type": "STRING",
        "source": "cc_num (pseudonymisé)",
        "comment": "Identifiant pseudonymisé du porteur de carte : empreinte SHA-256 salée du numéro de carte. Déterministe, donc stable entre exécutions et entre les périodes d'entraînement et de test.",
    },
    "customer_gender": {
        "type": "STRING",
        "source": "gender",
        "comment": "Genre déclaré du porteur (F ou M).",
    },
    "customer_age": {
        "type": "INT",
        "source": "dérivée de dob et event_time",
        "comment": "Âge du porteur en années révolues au moment de la transaction. La date de naissance elle-même n'est pas conservée (minimisation des données).",
    },
    "customer_job": {
        "type": "STRING",
        "source": "job",
        "comment": "Profession déclarée du porteur.",
    },
    "customer_city": {
        "type": "STRING",
        "source": "city",
        "comment": "Ville de résidence du porteur.",
    },
    "customer_state": {
        "type": "STRING",
        "source": "state",
        "comment": "État américain de résidence du porteur (code à deux lettres).",
    },
    "customer_zip": {
        "type": "STRING",
        "source": "zip (réparé)",
        "comment": "Code postal du porteur, sur 5 chiffres. Le zéro de tête, détruit en amont par un traitement numérique, est restauré par le pipeline (6,28 % des lignes concernées).",
    },
    "customer_city_population": {
        "type": "INT",
        "source": "city_pop",
        "comment": "Population de la ville de résidence du porteur.",
    },
    "customer_lat": {
        "type": "DOUBLE",
        "source": "lat",
        "comment": "Latitude du domicile du porteur.",
    },
    "customer_lon": {
        "type": "DOUBLE",
        "source": "long",
        "comment": "Longitude du domicile du porteur. Renommée depuis long, mot réservé en SQL.",
    },
    "merchant_name": {
        "type": "STRING",
        "source": "merchant (préfixe retiré)",
        "comment": "Raison sociale du commerçant. Le préfixe technique « fraud_ », présent sur la totalité des commerçants de la source, a été retiré après vérification qu'il n'est pas corrélé à la variable cible.",
    },
    "merchant_category": {
        "type": "STRING",
        "source": "category",
        "comment": "Catégorie d'achat du commerçant (14 modalités : alimentaire, carburant, santé, voyage, etc.).",
    },
    "merchant_lat": {
        "type": "DOUBLE",
        "source": "merch_lat",
        "comment": "Latitude du point de vente.",
    },
    "merchant_lon": {
        "type": "DOUBLE",
        "source": "merch_long",
        "comment": "Longitude du point de vente.",
    },
    "amount": {
        "type": "DOUBLE",
        "source": "amt",
        "comment": "Montant de la transaction, en dollars américains.",
    },
    "is_fraud": {
        "type": "INT",
        "source": "is_fraud",
        "comment": "Variable cible. Vaut 1 si la transaction est frauduleuse, 0 sinon. Classe positive fortement minoritaire (0,52 % du corpus).",
    },
    "flag_minor_cardholder": {
        "type": "BOOLEAN",
        "source": "dérivée",
        "comment": "Indicateur de qualité : le porteur avait moins de 18 ans au moment de la transaction. La ligne est conservée — un mineur peut légitimement être porteur secondaire d'une carte.",
    },
    "flag_zip_code_repaired": {
        "type": "BOOLEAN",
        "source": "dérivée",
        "comment": "Indicateur de qualité : le code postal reçu ne comportait que 4 caractères et son zéro de tête a été restauré par le pipeline.",
    },
    "_ingested_at": {
        "type": "TIMESTAMP",
        "source": "Bronze",
        "comment": "Horodatage d'entrée de la ligne dans la plateforme (couche Bronze).",
    },
    "_source_file": {
        "type": "STRING",
        "source": "Bronze",
        "comment": "Nom du fichier source dont la ligne est issue.",
    },
    "_ingestion_id": {
        "type": "STRING",
        "source": "Bronze",
        "comment": "Identifiant du lot d'ingestion ayant produit la ligne.",
    },
    "_silver_processed_at": {
        "type": "TIMESTAMP",
        "source": "dérivée",
        "comment": "Horodatage du traitement de nettoyage et de mise en conformité (couche Silver).",
    },
}

# Vue aplatie utilisée pour poser les commentaires Unity Catalog.
SILVER_COLUMN_COMMENTS: dict[str, str] = {
    column: doc["comment"] for column, doc in SILVER_DATA_DICTIONARY.items()
}


def column_comment_statements(table: str, comments: dict[str, str]) -> list[str]:
    """Génère les instructions SQL posant les commentaires sur les colonnes.

    Les apostrophes sont doublées pour être échappées en SQL — le français en
    contient beaucoup.
    """
    return [
        f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{comment.replace(chr(39), chr(39) * 2)}'"
        for column, comment in comments.items()
    ]
