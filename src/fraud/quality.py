"""Moteur générique de règles de qualité de données.

Principe directeur : **aucune ligne n'est jamais supprimée silencieusement**.

Un filtre du type `WHERE amount > 0` est invisible. Personne ne saura combien de
transactions ont disparu, ni pourquoi, ni quand le taux de rejet s'est mis à
dériver. En redirigeant les lignes non conformes vers une table de quarantaine
accompagnées du motif exact de leur rejet, le taux de rejet devient une
**métrique de qualité mesurable, historisée et présentable**.

Le moteur distingue deux sévérités :

- ``REJECT`` — la ligne est structurellement inexploitable (horodatage
  illisible, montant non numérique, cible hors domaine). Elle part en
  quarantaine.
- ``FLAG`` — la ligne est exploitable mais présente une anomalie métier
  (porteur mineur, par exemple). Elle est **conservée** et marquée d'un
  indicateur booléen.

La distinction est importante et se défend : rejeter une ligne pour une
anomalie métier reviendrait à décider, au nom du métier, que la donnée n'existe
pas. Un porteur de 15 ans est peut-être un porteur secondaire parfaitement
légitime — ce n'est pas à la couche technique de trancher. On signale, on
compte, et on laisse l'analyse décider.

Ce module ne connaît rien au domaine de la fraude : il est réutilisable tel
quel pour les contrôles de la couche Gold.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Colonnes techniques ajoutées par le moteur.
REJECTION_REASONS_COL = "_rejection_reasons"
QUARANTINED_AT_COL = "_quarantined_at"


class Severity(str, Enum):
    """Conséquence de l'infraction à une règle."""

    REJECT = "REJECT"  # ligne redirigée vers la quarantaine
    FLAG = "FLAG"  # ligne conservée, marquée d'un indicateur


# `eq=False` : le dataclass contient une Column Spark. La comparaison générée
# par défaut appellerait `==` dessus, ce qui produit une Column (une expression)
# au lieu d'un booléen, et lèverait une erreur à l'évaluation.
@dataclass(frozen=True, eq=False)
class QualityRule:
    """Une règle de qualité, exprimée par la condition qui la viole.

    Attributes:
        name: identifiant technique, en anglais et en snake_case. Sert de motif
            de rejet dans la quarantaine et de nom de colonne pour les
            indicateurs — il apparaît donc dans les données, pas seulement dans
            le code.
        description: formulation métier en français, reprise telle quelle dans
            le rapport de qualité et dans le mémoire.
        severity: voir :class:`Severity`.
        violated_when: expression Spark **vraie lorsque la règle est enfreinte**.
            Formuler la condition à l'envers (« quand est-ce anormal ? ») plutôt
            qu'à l'endroit évite les doubles négations dans les expressions.
    """

    name: str
    description: str
    severity: Severity
    violated_when: Column


def _violated_rule_names(rules: list[QualityRule]) -> Column:
    """Construit le tableau des noms de règles enfreintes par chaque ligne."""
    candidates = F.array(
        *[F.when(rule.violated_when, F.lit(rule.name)) for rule in rules]
    )
    # Les règles respectées produisent NULL : on ne garde que les infractions.
    return F.filter(candidates, lambda name: name.isNotNull())


def split_on_quality(
    df: DataFrame, reject_rules: list[QualityRule]
) -> tuple[DataFrame, DataFrame]:
    """Sépare les lignes conformes des lignes à mettre en quarantaine.

    Une ligne peut enfreindre plusieurs règles à la fois. Toutes les infractions
    sont conservées dans ``_rejection_reasons`` — s'arrêter à la première
    priverait l'analyse du diagnostic complet.

    Returns:
        Le couple (conformes, quarantaine). Les lignes en quarantaine portent
        deux colonnes supplémentaires : ``_rejection_reasons`` et
        ``_quarantined_at``.
    """
    tagged = df.withColumn(REJECTION_REASONS_COL, _violated_rule_names(reject_rules))

    clean = tagged.filter(F.size(REJECTION_REASONS_COL) == 0).drop(REJECTION_REASONS_COL)
    quarantined = tagged.filter(F.size(REJECTION_REASONS_COL) > 0).withColumn(
        QUARANTINED_AT_COL, F.current_timestamp()
    )
    return clean, quarantined


def apply_flags(df: DataFrame, flag_rules: list[QualityRule]) -> DataFrame:
    """Ajoute une colonne booléenne par règle de sévérité ``FLAG``."""
    for rule in flag_rules:
        df = df.withColumn(rule.name, rule.violated_when)
    return df


def violation_counts(df: DataFrame, rules: list[QualityRule]) -> list[dict]:
    """Compte les infractions à chaque règle, en une seule passe sur les données.

    Le comptage est agrégé en une unique action Spark plutôt qu'en une par
    règle : sur 1,85 M de lignes, la différence entre une passe et une dizaine
    n'est pas anecdotique.

    Returns:
        Une liste de dictionnaires ``{rule, severity, description, violations,
        rate_pct}``, dans l'ordre de déclaration des règles.
    """
    if not rules:
        return []

    total = df.count()
    aggregated = df.agg(
        *[
            F.sum(F.when(rule.violated_when, 1).otherwise(0)).alias(rule.name)
            for rule in rules
        ]
    ).collect()[0]

    return [
        {
            "rule": rule.name,
            "severity": rule.severity.value,
            "description": rule.description,
            "violations": int(aggregated[rule.name] or 0),
            "rate_pct": round(100 * (aggregated[rule.name] or 0) / total, 4) if total else 0.0,
        }
        for rule in rules
    ]


def format_report(counts: list[dict], total_rows: int) -> str:
    """Met en forme le rapport de qualité pour affichage en notebook.

    Ce rapport n'est pas décoratif : c'est la trace d'exécution des contrôles.
    Un contrôle qui ne rejette rien reste un contrôle utile — il **prouve** la
    conformité au lieu de la supposer, et il protégera le pipeline le jour où la
    source changera.
    """
    lines = [
        f"Rapport de qualité — {total_rows:,} lignes contrôlées".replace(",", " "),
        "=" * 92,
        f"{'Règle':<32}{'Sévérité':<10}{'Infractions':>14}{'Taux':>10}   Statut",
        "-" * 92,
    ]
    for row in counts:
        status = "OK" if row["violations"] == 0 else "A EXAMINER"
        lines.append(
            f"{row['rule']:<32}{row['severity']:<10}"
            f"{row['violations']:>14,}{row['rate_pct']:>9.4f}%   {status}".replace(",", " ")
        )
    lines.append("-" * 92)
    for row in counts:
        lines.append(f"  {row['rule']} : {row['description']}")
    return "\n".join(lines)
