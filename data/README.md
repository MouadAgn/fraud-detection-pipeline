# Données

Ce dossier est **volontairement exclu du versionnement** (`.gitignore`) :

- le dataset pèse ~500 Mo décompressé, ce qui n'a pas sa place dans Git ;
- la licence Kaggle n'autorise pas la redistribution du jeu de données.

## Récupérer les données

### Option 1 — script fourni (recommandé)

```powershell
# Prérequis : compte Kaggle + fichier ~/.kaggle/kaggle.json
# (Kaggle → Settings → API → Create New Token)
pip install kaggle
.\scripts\download_dataset.ps1
```

### Option 2 — téléchargement manuel

<https://www.kaggle.com/datasets/kartik2112/fraud-detection> → bouton *Download*,
puis décompresser dans `data/raw/`.

## Arborescence attendue

```
data/
└── raw/
    ├── fraudTrain.csv
    └── fraudTest.csv
```

## Destination réelle

Ces fichiers ne servent qu'à l'inspection locale et à l'upload initial.
Le pipeline lit les données depuis un **volume Unity Catalog** sur Databricks :

```
/Volumes/workspace/fraud_detection/raw/
```

Voir [docs/memoire/00_contexte_et_donnees.md](../docs/memoire/00_contexte_et_donnees.md)
pour la description du jeu de données et la justification de son choix.
