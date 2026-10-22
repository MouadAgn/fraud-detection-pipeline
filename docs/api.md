# API locale de détection de fraude

Cette API utilise le modèle entraîné, sans relancer le pipeline Databricks.
Elle reçoit l'identifiant d'une transaction présente dans un export Gold de
100 transactions test. Les variables historiques restent calculées par Spark ;
l'API ne prétend pas scorer une transaction brute en temps réel.

## 1. Démarrer le service sur ton Mac

Depuis la racine du projet :

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-api.txt
python -m uvicorn fraud_api.app:app --app-dir src --host 127.0.0.1 --port 8000
```

Ouvrir http://127.0.0.1:8000/docs pour la documentation interactive.
`GET /health` répond `{"status":"ok"}` : le serveur fonctionne.
`GET /ready` répond 503 tant que le modèle et l'échantillon ne sont pas chargés.
`POST /predict` répond aussi 503 dans ce cas, sans inventer de score.

## 2. Récupérer le vrai modèle et ses données

Dans Databricks, après le notebook `05_scoring_eval`, exécuter
`09_export_api_bundle.py`. Ce notebook exporte le modèle sklearn complet
(prétraitement inclus), sa version numérique, les variables déjà préparées,
le seuil vérifié et des scores de référence calculés dans Databricks.
Il recalcule les features et la validation comme le notebook 05, ce qui peut
prendre quelques minutes. Il n'altère pas les tables ni l'alias champion.

Télécharger `api_bundle.zip` depuis le volume du projet affiché par le notebook,
puis décompresser son dossier `api_bundle` dans `data/` :

```text
data/api_bundle/
  metadata.json
  transactions.json
  reference_predictions.json
  model/
    MLmodel
    requirements.txt
    ...
```

Installer **les dépendances exactes de l'artefact d'entraînement**, puis celles
du serveur dans un environnement Python compatible avec l'artefact. La version
Python et les bibliothèques sont indiquées dans `model/python_env.yaml`,
`model/MLmodel` et `model/requirements.txt`. Si la version de Python diffère,
créer l'environnement avec la bonne version avant d'installer :

```bash
python -m pip install -r data/api_bundle/model/requirements.txt
python -m pip install -r requirements-api.txt
```

Résoudre toute incompatibilité de versions avant de démarrer le service.
Charger uniquement ton propre export de confiance : un artefact sklearn
sérialisé peut exécuter du code au chargement.
Les données et modèles dans `data/` sont déjà exclus de Git.

Redémarrer ensuite le serveur. Un autre emplacement se configure via
`FRAUD_BUNDLE_PATH`. `GET /ready` doit alors répondre 200.

## 3. Demander une prédiction

Copier un identifiant de `reference_predictions.json` puis l'utiliser dans
`POST /predict` via `/docs`, ou avec curl :

```bash
curl -X POST http://127.0.0.1:8000/predict \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"REMPLACER_PAR_UN_IDENTIFIANT_EXPORTE"}'
```

La réponse contient le score, la décision `is_flagged` (score >= seuil), le
seuil, la version du modèle et la date de l'export. Un identifiant inconnu
renvoie 404. Comparer le score aux prédictions de référence Databricks pour
valider la portabilité du vrai modèle avant toute livraison.

## Tests

```bash
python -m pip install -r requirements-api-dev.txt
python -m pytest tests/test_api.py -v
```

Les tests MLflow utilisent un petit modèle synthétique entraîné dans le test,
jamais présenté comme le modèle du mémoire. Ils vérifient la conservation des
scores après export/rechargement, le seuil, les erreurs et l'absence de faux
résultats si l'export manque. Installer `requirements-dev.txt` pour exécuter
également les tests de métriques existants.

## Limite de cette première étape

Le serveur écoute uniquement sur ton Mac. Il n'a pas encore d'authentification
ni de déploiement Azure. Son export est figé ; il ne consulte pas les données
bancaires en direct. Docker est décrit ci-dessous. ACR, ACI et GitHub Actions viendront dans les
prochaines étapes de l'apprentissage.

## Exécuter dans Docker

Le Dockerfile installe les versions du modèle exporté, copie l'API et le
bundle, puis démarre le serveur avec un utilisateur non administrateur.
Le `.dockerignore` exclut les fichiers Git, secrets, environnements Python,
notebooks et jeux de données bruts. L'export local `data/api_bundle` est requis
avant la construction ; il reste exclu de Git, même s'il est inclus dans l'image.
Pour une future CI, cet export devra être récupéré comme un artefact versionné.

Depuis la racine du projet, Docker Desktop démarré :

```bash
# ACI attend une image Linux x64, même depuis un Mac Apple Silicon.
docker build --platform linux/amd64 -t fraud-api:local .
# 8001 sur le Mac pour conserver l'API Python existante sur 8000.
docker run -d --name fraud-api-local --platform linux/amd64 \
  -p 127.0.0.1:8001:8000 fraud-api:local
```

Ouvrir http://127.0.0.1:8001/docs. Les mêmes identifiants de transaction et
scores doivent fonctionner. `docker ps` affiche l'état de santé ; le contrôle
utilise `/ready`, pas seulement la présence du serveur HTTP.

```bash
docker logs fraud-api-local
docker stop fraud-api-local
# Supprimer seulement le conteneur arrêté, conserver l'image :
docker rm fraud-api-local
```

L'API du conteneur est liée à l'interface locale du Mac. Aucun push dans ACR
ni déploiement sur ACI n'est effectué par ces commandes. L'image embarque un
échantillon figé et le modèle pour cette démonstration pédagogique.
