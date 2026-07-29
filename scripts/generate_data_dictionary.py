"""Génère `docs/dictionnaire_donnees.md` depuis le code du pipeline.

Le dictionnaire de données est **dérivé**, jamais rédigé à la main. Documenter
les colonnes à deux endroits — dans le code et dans un document Markdown —
garantit qu'ils divergeront à la première modification. Ici, la source unique de
vérité est `SILVER_DATA_DICTIONARY` dans `src/fraud/silver.py`, qui alimente
simultanément :

- les commentaires posés sur les colonnes dans Unity Catalog (notebook 02) ;
- le document Markdown produit par ce script.

Le module `silver.py` importe PySpark, indisponible hors d'un environnement
Databricks. Le dictionnaire est donc extrait par **analyse syntaxique** du
fichier plutôt que par import : on lit la structure du code sans l'exécuter.

Usage :
    py -3.13 scripts/generate_data_dictionary.py
"""

from __future__ import annotations

import ast
import contextlib
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SILVER_MODULE = REPO_ROOT / "src" / "fraud" / "silver.py"
OUTPUT = REPO_ROOT / "docs" / "dictionnaire_donnees.md"

DICTIONARY_NAME = "SILVER_DATA_DICTIONARY"
DROPPED_NAME = "DROPPED_COLUMNS"


def extract_dict(module_path: Path, variable: str) -> dict:
    """Extrait un dictionnaire littéral d'un module, sans l'importer.

    `ast.literal_eval` n'évalue que des littéraux : aucune ligne du module n'est
    exécutée, ce qui permet de lire un fichier dépendant de PySpark depuis un
    poste qui n'en dispose pas.
    """
    tree = ast.parse(module_path.read_text(encoding="utf-8"))

    for node in tree.body:
        # `NOM: type = {...}` (AnnAssign) ou `NOM = {...}` (Assign)
        target = None
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target = node.target.id
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            if isinstance(node.targets[0], ast.Name):
                target = node.targets[0].id

        if target == variable and node.value is not None:
            return ast.literal_eval(node.value)

    raise KeyError(f"Variable '{variable}' introuvable dans {module_path}")


def escape_cell(text: str) -> str:
    """Neutralise les caractères qui casseraient une cellule de tableau Markdown."""
    return text.replace("|", r"\|").replace("\n", " ")


def render(columns: dict, dropped: dict) -> str:
    lines = [
        "# Dictionnaire de données",
        "",
        "> **Document généré** — ne pas modifier à la main.",
        "> Source de vérité : `SILVER_DATA_DICTIONARY` dans "
        "[`src/fraud/silver.py`](../src/fraud/silver.py).",
        "> Régénérer avec `py -3.13 scripts/generate_data_dictionary.py`.",
        "",
        f"*Dernière génération : {date.today().isoformat()}*",
        "",
        "---",
        "",
        "## Pourquoi ce document",
        "",
        "Les noms de colonnes du pipeline sont en **anglais** : c'est le standard "
        "professionnel, et c'est aussi la langue des colonnes de la source. La "
        "documentation fonctionnelle est en **français**, langue du mémoire.",
        "",
        "Ces mêmes descriptions sont posées comme commentaires sur les colonnes "
        "dans Unity Catalog : elles s'affichent directement dans le Catalog "
        "Explorer de Databricks, au plus près de la donnée.",
        "",
        "Au-delà de la question de la langue, un dictionnaire de données est un "
        "livrable attendu en contexte bancaire réglementé : il documente la "
        "signification et la provenance de chaque champ, pour les auditeurs "
        "comme pour les analystes.",
        "",
        "---",
        "",
        "## Table `silver_transactions`",
        "",
        f"{len(columns)} colonnes.",
        "",
        "| Colonne | Type | Origine | Description |",
        "| --- | --- | --- | --- |",
    ]

    for name, doc in columns.items():
        lines.append(
            f"| `{name}` | {doc['type']} | `{escape_cell(doc['source'])}` "
            f"| {escape_cell(doc['comment'])} |"
        )

    lines += [
        "",
        "---",
        "",
        "## Colonnes de Bronze non reprises en Silver",
        "",
        "Toute suppression est une décision, et toute décision se justifie.",
        "",
        "| Colonne source | Motif de suppression |",
        "| --- | --- |",
    ]
    for name, reason in dropped.items():
        lines.append(f"| `{name}` | {escape_cell(reason)} |")

    lines += [
        "",
        "---",
        "",
        "## Conventions",
        "",
        "- Les colonnes préfixées **`_`** portent la traçabilité technique "
        "(ingestion, traitement) et non de la donnée métier.",
        "- Les colonnes préfixées **`flag_`** sont des indicateurs de qualité : "
        "elles signalent une anomalie sans écarter la ligne.",
        "- Les colonnes préfixées **`customer_`** et **`merchant_`** lèvent "
        "l'ambiguïté de la source, où `lat`/`long` et `merch_lat`/`merch_long` "
        "désignaient deux entités différentes sans le dire.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    with contextlib.suppress(AttributeError):
        sys.stdout.reconfigure(encoding="utf-8")

    columns = extract_dict(SILVER_MODULE, DICTIONARY_NAME)
    dropped = extract_dict(SILVER_MODULE, DROPPED_NAME)

    OUTPUT.write_text(render(columns, dropped), encoding="utf-8")

    print(f"Écrit : {OUTPUT.relative_to(REPO_ROOT)}")
    print(f"  {len(columns)} colonnes documentées")
    print(f"  {len(dropped)} colonnes supprimées documentées")


if __name__ == "__main__":
    main()
