# Python 3.12 comme l'environnement d'entraînement Databricks.
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src \
    FRAUD_BUNDLE_PATH=/app/data/api_bundle \
    OMP_NUM_THREADS=1

WORKDIR /app

# XGBoost nécessite OpenMP. gcc et les en-têtes servent à compiler psutil 5.9.0.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Conserver les versions de bibliothèques enregistrées avec le vrai modèle.
COPY data/api_bundle/model/requirements.txt /tmp/model-requirements.txt
COPY requirements-api.txt /tmp/api-requirements.txt
RUN python -m pip install -r /tmp/model-requirements.txt -r /tmp/api-requirements.txt \
    && python -m pip check \
    && apt-get purge -y --auto-remove gcc libc6-dev

# Copier seulement le service et l'export, pas les notebooks ni les données brutes.
COPY src/fraud_api/ /app/src/fraud_api/
COPY data/api_bundle/ /app/data/api_bundle/

RUN groupadd --gid 10001 api \
    && useradd --uid 10001 --gid api --no-log-init --create-home api
USER 10001:10001

EXPOSE 8000

# L'image est saine uniquement quand le modèle ET les données sont chargés.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=3)" || exit 1

CMD ["python", "-m", "uvicorn", "fraud_api.app:app", "--host", "0.0.0.0", "--port", "8000"]
