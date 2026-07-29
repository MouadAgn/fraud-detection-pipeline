"""Schémas explicites et contrat de données du pipeline.

Pourquoi un schéma explicite plutôt que `inferSchema=True` ?

1. **Coût** — l'inférence oblige Spark à lire l'intégralité du fichier une
   première fois rien que pour deviner les types, puis une seconde fois pour
   charger les données. Sur 501 Mo, c'est un doublement du temps d'ingestion
   sans contrepartie.
2. **Stabilité** — le type inféré dépend du contenu du fichier. Si un futur
   lot contient une valeur alphanumérique dans une colonne jusque-là numérique,
   le type change silencieusement et tout l'aval casse. Un schéma déclaré est
   un contrat : ce qui ne s'y conforme pas est détecté, pas absorbé.
3. **Traçabilité** — le schéma versionné dans Git documente ce que le pipeline
   attend de sa source. C'est une information de conception, pas un détail
   d'exécution.

Pourquoi toutes les colonnes en `STRING` en couche Bronze ?

Un fichier CSV ne porte aucun type : tout y est du texte. Déclarer `amt` comme
un décimal dès la lecture, c'est déjà **interpréter** la donnée — ce qui n'est
pas le rôle de Bronze.

Le risque est concret. En mode `PERMISSIVE` (le défaut de Spark), une valeur
qui ne se convertit pas au type déclaré est remplacée par `NULL`, **sans
erreur**. On perdrait donc de la donnée de façon invisible, à l'endroit précis
du pipeline censé garantir la fidélité à la source.

Le cas le plus parlant est `cc_num` : lu comme un entier, `2703186189652095`
passe encore, mais un numéro de carte est un **identifiant**, pas une quantité.
Un zéro de tête serait perdu, un numéro à 19 chiffres déborderait. Aucune
arithmétique n'a de sens sur cette colonne : elle doit rester du texte.

Le typage est donc une responsabilité de la couche **Silver**, où il devient un
contrôle explicite et mesurable (cf. `docs/memoire/03_qualite_silver.md`).
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Contrat de données de la source
# ---------------------------------------------------------------------------
# En-tête exact du fichier CSV, tel qu'il est écrit sur disque.
# Noter la virgule en tête : la première colonne n'a PAS de nom dans le
# fichier. Il s'agit de l'index de ligne ajouté par l'export pandas d'origine.
EXPECTED_RAW_HEADER: str = (
    ",trans_date_trans_time,cc_num,merchant,category,amt,first,last,gender,"
    "street,city,state,zip,lat,long,city_pop,job,dob,trans_num,unix_time,"
    "merch_lat,merch_long,is_fraud"
)

# Noms attribués aux colonnes, dans l'ordre exact du fichier.
# Seule la colonne anonyme est nommée (`source_row_index`) : lui donner un nom
# est indispensable pour pouvoir l'écrire en Delta. Toutes les autres
# conservent leur nom d'origine — Bronze reste fidèle à la source, y compris
# dans sa nomenclature.
SOURCE_COLUMN_ORDER: tuple[str, ...] = (
    "source_row_index",  # index technique de l'export CSV, supprimé en Silver
    "trans_date_trans_time",  # horodatage de référence du pipeline
    "cc_num",
    "merchant",
    "category",
    "amt",
    "first",
    "last",
    "gender",
    "street",
    "city",
    "state",
    "zip",
    "lat",
    "long",
    "city_pop",
    "job",
    "dob",
    "trans_num",
    "unix_time",  # incohérent avec l'horodatage (décalage de 7 ans), recalculé en Silver
    "merch_lat",
    "merch_long",
    "is_fraud",  # variable cible
)

# Schéma de lecture : tout en texte (voir docstring du module).
BRONZE_SOURCE_SCHEMA: StructType = StructType(
    [StructField(name, StringType(), nullable=True) for name in SOURCE_COLUMN_ORDER]
)

# ---------------------------------------------------------------------------
# Colonnes de traçabilité ajoutées à l'ingestion
# ---------------------------------------------------------------------------
# Préfixées par `_` pour les distinguer sans ambiguïté des colonnes métier.
# Ce sont elles qui rendent le pipeline auditable : sans elles, impossible de
# répondre à « d'où vient cette ligne et quand est-elle entrée ? ».
INGESTION_METADATA_COLUMNS: tuple[str, ...] = (
    "_ingested_at",  # horodatage UTC de l'ingestion
    "_source_file",  # fichier d'origine — distingue train et test dans la table
    "_ingestion_id",  # identifiant du lot, corrèle les lignes d'un même run
)

# ---------------------------------------------------------------------------
# Volumétrie attendue — contrôle de réconciliation source ↔ cible
# ---------------------------------------------------------------------------
# Chiffres établis par `scripts/profile_dataset.py`. Comparer le nombre de
# lignes écrites à ces valeurs est un contrôle de bout en bout : il détecte
# aussi bien un fichier tronqué qu'un parsing CSV défaillant (un mauvais
# traitement des guillemets décale les colonnes ET change le nombre de lignes).
EXPECTED_ROW_COUNTS: dict[str, int] = {
    "fraudTrain.csv": 1_296_675,
    "fraudTest.csv": 555_719,
}
EXPECTED_TOTAL_ROWS: int = sum(EXPECTED_ROW_COUNTS.values())  # 1 852 394
EXPECTED_TOTAL_FRAUDS: int = 9_651


class SchemaValidationError(Exception):
    """Levée quand la source ne respecte plus le contrat de données déclaré."""


def validate_raw_header(actual_header: str) -> None:
    """Vérifie que l'en-tête du fichier source correspond au contrat attendu.

    Ce contrôle protège contre le scénario le plus dangereux de l'ingestion
    CSV : une source dont l'ordre des colonnes change. Comme le schéma est
    appliqué par **position**, une inversion de deux colonnes de même type
    passerait totalement inaperçue — les données seraient corrompues sans la
    moindre erreur, et le défaut ne se révélerait qu'au niveau des résultats
    du modèle, des semaines plus tard.

    Args:
        actual_header: première ligne du fichier CSV, sans le retour à la ligne.

    Raises:
        SchemaValidationError: si l'en-tête diffère du contrat.
    """
    actual = actual_header.strip().lstrip("﻿")  # retire un éventuel BOM UTF-8
    if actual == EXPECTED_RAW_HEADER:
        return

    expected_cols = EXPECTED_RAW_HEADER.split(",")
    actual_cols = actual.split(",")

    details = []
    if len(expected_cols) != len(actual_cols):
        details.append(
            f"nombre de colonnes : attendu {len(expected_cols)}, reçu {len(actual_cols)}"
        )
    for position, (expected, found) in enumerate(zip(expected_cols, actual_cols)):
        if expected != found:
            details.append(
                f"position {position} : attendu '{expected}', reçu '{found}'"
            )

    raise SchemaValidationError(
        "L'en-tête du fichier source ne respecte pas le contrat de données.\n"
        + "\n".join(f"  - {d}" for d in details)
    )
