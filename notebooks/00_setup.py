# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Mise en place de l'environnement Unity Catalog
# MAGIC
# MAGIC **Objectif** : créer les objets de stockage du projet et vérifier que le
# MAGIC notebook accède bien au code versionné du dépôt.
# MAGIC
# MAGIC Ce notebook est **idempotent** : toutes les créations utilisent
# MAGIC `IF NOT EXISTS`, il peut donc être relancé sans risque. C'est une exigence
# MAGIC de base pour tout traitement destiné à être orchestré — un job planifié
# MAGIC qui échoue à mi-parcours doit pouvoir être relancé tel quel.
# MAGIC
# MAGIC ## Ce qu'on crée, et pourquoi
# MAGIC
# MAGIC | Objet | Rôle |
# MAGIC |---|---|
# MAGIC | **Schéma** `fraud_detection` | Regroupe toutes les tables du projet. Un projet = un schéma : écrire dans `default` mélangerait les projets et rendrait la gouvernance impossible. |
# MAGIC | **Volume** `raw` | Stockage des fichiers non tabulaires (les CSV sources), géré par Unity Catalog. Remplace DBFS, déprécié, et bénéficie du contrôle d'accès d'Unity Catalog. |
# MAGIC
# MAGIC > **Note sur les volumes** — un volume est un emplacement de fichiers
# MAGIC > gouverné, accessible par un chemin classique `/Volumes/<catalogue>/<schéma>/<volume>/`.
# MAGIC > C'est le mécanisme recommandé pour déposer des fichiers bruts : contrairement
# MAGIC > à DBFS, les droits d'accès y sont gérés par Unity Catalog et les accès sont
# MAGIC > tracés.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Accès au code du dépôt
# MAGIC
# MAGIC Le notebook importe la configuration depuis `src/fraud/config.py` plutôt que
# MAGIC de redéclarer les noms de tables. Ainsi, un seul fichier fait autorité sur la
# MAGIC nomenclature, et les cinq notebooks du pipeline ne peuvent pas diverger.

# COMMAND ----------

import os
import sys

# Dans un Git folder Databricks, le répertoire courant d'un notebook est le
# dossier qui le contient (`notebooks/`). La racine du dépôt est son parent.
REPO_ROOT = os.path.dirname(os.getcwd())
SRC_PATH = os.path.join(REPO_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

print(f"Racine du dépôt : {REPO_ROOT}")
print(f"Chemin du code  : {SRC_PATH}")

if not os.path.isdir(SRC_PATH):
    raise RuntimeError(
        f"Dossier 'src' introuvable sous {REPO_ROOT}.\n"
        "Vérifier que ce notebook est exécuté depuis un Git folder contenant "
        "l'ensemble du dépôt, et non copié isolément dans le workspace."
    )

# COMMAND ----------

from fraud import config

print(config.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vérification du catalogue
# MAGIC
# MAGIC Sur Databricks Free Edition, le catalogue par défaut du workspace s'appelle
# MAGIC `workspace`. La cellule ci-dessous liste les catalogues réellement
# MAGIC accessibles : si `workspace` n'y figure pas, il faut ajuster `FRAUD_CATALOG`
# MAGIC dans `src/fraud/config.py` avant d'aller plus loin.

# COMMAND ----------

catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
print("Catalogues accessibles :", catalogs)

if config.CATALOG not in catalogs:
    raise RuntimeError(
        f"Le catalogue '{config.CATALOG}' n'est pas accessible.\n"
        f"Catalogues disponibles : {catalogs}\n"
        "Corriger la constante CATALOG dans src/fraud/config.py."
    )

print(f"OK — catalogue '{config.CATALOG}' accessible.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Création du schéma et du volume

# COMMAND ----------

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.SCHEMA}
    COMMENT 'Détection de fraude sur transactions par carte — architecture Medallion (mémoire MIA26)'
""")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {config.CATALOG}.{config.SCHEMA}.{config.VOLUME}
    COMMENT 'Fichiers sources bruts (CSV Sparkov) avant ingestion en couche Bronze'
""")

print(f"Schéma  : {config.CATALOG}.{config.SCHEMA}")
print(f"Volume  : {config.VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Contrôle : les fichiers sources sont-ils en place ?
# MAGIC
# MAGIC Le volume vient d'être créé, il est donc normal qu'il soit vide au premier
# MAGIC passage. Les deux fichiers `fraudTrain.csv` et `fraudTest.csv` doivent y être
# MAGIC déposés avant d'exécuter le notebook `01_bronze_ingestion`.

# COMMAND ----------

EXPECTED_FILES = {"fraudTrain.csv", "fraudTest.csv"}

files = {f.name: f.size for f in dbutils.fs.ls(config.VOLUME_PATH)}

if files:
    for name, size in sorted(files.items()):
        print(f"  {name:<20} {size / 1024**2:>8.1f} Mo")
else:
    print("  (volume vide)")

missing = EXPECTED_FILES - set(files)
if missing:
    print()
    print(f"MANQUANT : {', '.join(sorted(missing))}")
    print(f"Déposer les fichiers dans : {config.VOLUME_PATH}")
    print("Catalog > workspace > fraud_detection > Volumes > raw > Upload to this volume")
else:
    print()
    print("OK — les deux fichiers sources sont présents.")
    print("Étape suivante : exécuter le notebook 01_bronze_ingestion.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Récapitulatif
# MAGIC
# MAGIC Objets créés (ou déjà existants) :
# MAGIC
# MAGIC - `workspace.fraud_detection` — schéma du projet
# MAGIC - `workspace.fraud_detection.raw` — volume des fichiers sources
# MAGIC
# MAGIC **Point à savoir expliquer** : pourquoi un volume Unity Catalog plutôt que
# MAGIC DBFS ? Parce que DBFS n'offre aucun contrôle d'accès granulaire — tout
# MAGIC utilisateur du workspace y accède intégralement. Un volume est un objet
# MAGIC gouverné : les droits se posent dessus comme sur une table, et les accès sont
# MAGIC tracés. Dans un contexte bancaire, cette différence n'est pas un détail.
