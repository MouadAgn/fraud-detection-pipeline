# Orchestration

Le pipeline est décrit dans [`databricks.yml`](../databricks.yml) à la racine du dépôt, au format **Databricks Asset Bundle**.

## Pourquoi un fichier versionné plutôt que l'interface graphique

Un enchaînement construit dans l'interface n'existe que dans le workspace. Il ne se relit pas en revue de code, ne se rejoue pas sur un autre environnement, ne se compare pas d'une version à l'autre, et disparaît avec le workspace.

Décrit dans un fichier versionné, le pipeline devient un **artefact du projet** : son historique de modifications est traçable, son déploiement reproductible, et sa revue possible.

## Deux jobs, et non un seul

C'est la décision structurante de cette étape.

| Job | Contenu | Fréquence |
| --- | --- | --- |
| **`fraud_detection_pipeline`** | setup → bronze → silver → gold → scoring | Quotidienne |
| **`fraud_model_training`** | Entraînement, comparaison, enregistrement | Mensuelle ou manuelle |

**Le pipeline de données et le cycle de vie du modèle n'ont pas la même fréquence.** Les données arrivent tous les jours ; un modèle se réentraîne quand la population a suffisamment dérivé.

Les enchaîner dans un même job produirait un modèle différent chaque jour — donc des décisions non reproductibles et des métriques incomparables d'un jour à l'autre. Le pipeline quotidien consomme la version portant l'alias `champion` ; le job d'entraînement est le seul à déplacer cet alias.

## Déploiement — avec le CLI Databricks

```bash
# Installation (Windows)
winget install Databricks.DatabricksCLI

# Authentification par navigateur, sans jeton à manipuler
databricks auth login --host https://<votre-workspace>.cloud.databricks.com

# Déploiement
export DATABRICKS_HOST=https://<votre-workspace>.cloud.databricks.com
databricks bundle validate
databricks bundle deploy --target dev

# Exécution immédiate, sans attendre la planification
databricks bundle run fraud_detection_pipeline
```

## Déploiement — sans le CLI, depuis l'interface

`Jobs & Pipelines` → **Create job**, puis ajouter les tâches dans cet ordre :

| Tâche | Notebook | Dépend de |
| --- | --- | --- |
| `setup` | `notebooks/00_setup` | — |
| `bronze_ingestion` | `notebooks/01_bronze_ingestion` | `setup` |
| `silver_cleaning` | `notebooks/02_silver_cleaning` | `bronze_ingestion` |
| `gold_features` | `notebooks/03_gold_features` | `silver_cleaning` |
| `scoring` | `notebooks/05_scoring_eval` | `gold_features` |

Pour chaque tâche : compute **Serverless**, et **1 tentative de reprise**.

Puis créer un second job `fraud_model_training` contenant la seule tâche `notebooks/04_model_training`, **sans reprise**.

> Le fichier `databricks.yml` reste la référence : si le job est modifié dans l'interface, reporter le changement dans le fichier, faute de quoi les deux divergent silencieusement.

## Choix de configuration, et pourquoi

| Réglage | Valeur | Justification |
| --- | --- | --- |
| `max_concurrent_runs` | 1 | Deux exécutions simultanées écriraient concurremment sur les mêmes tables Delta |
| `max_retries` (pipeline) | 1 | Les échecs sont des erreurs de données, pas des incidents transitoires. Réessayer dix fois masquerait le problème au lieu de le signaler |
| `max_retries` (entraînement) | 0 | Un entraînement dure une vingtaine de minutes et échoue pour des raisons qu'une nouvelle tentative ne corrigera pas |
| `pause_status` | `PAUSED` | Un job déployé ne doit pas se déclencher avant d'avoir été exécuté manuellement au moins une fois |
| Notification | à l'échec uniquement | Notifier chaque succès produit un bruit que plus personne ne lit — et l'échec passe alors inaperçu |

## Ce qui rend cette orchestration possible

Chaque notebook est **exécutable indépendamment** : il lit ses entrées depuis Unity Catalog et n'hérite d'aucune variable du précédent. Dans un Workflow, chaque tâche s'exécute dans sa propre session — un notebook qui dépendrait de l'état laissé par un autre ne pourrait tout simplement pas être orchestré.

Chaque notebook est également **idempotent** : toutes les écritures se font en `overwrite` sur des sources complètes. Une tâche qui échoue peut donc être relancée telle quelle, sans nettoyage préalable ni risque de doublon.

Ces deux propriétés n'ont pas été ajoutées à cette étape : elles ont été respectées dès l'étape 1, précisément pour rendre cette orchestration possible.
