# Stage 1: dependency builder
FROM python:3.11-slim-bookworm AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./

RUN pip install --no-cache-dir --prefix=/install \
        -r requirements.txt \
        google-cloud-secret-manager==2.20.0 \
        pyyaml==6.0.1

#  Stage 2: production image
FROM python:3.11-slim-bookworm AS production

LABEL maintainer="data-engineering@insureflow.io"
LABEL org.opencontainers.image.title="InsureFlow Airflow Worker"
LABEL org.opencontainers.image.description="Multi-directory SFTP ingestion pipeline"

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        gnupg \
        libpq5 \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Non-root user
ARG AIRFLOW_UID=5000
ARG AIRFLOW_GID=5000
RUN groupadd --gid ${AIRFLOW_GID} airflow \
 && useradd  --uid ${AIRFLOW_UID} \
             --gid ${AIRFLOW_GID} \
             --home-dir /opt/airflow \
             --shell /bin/bash \
             --no-create-home \
             airflow

# Python packages from builder
COPY --from=builder /install /usr/local

# App source
WORKDIR /opt/airflow

COPY --chown=airflow:airflow dags/           ./dags/
COPY --chown=airflow:airflow pipeline/       ./pipeline/
COPY --chown=airflow:airflow schemas/        ./schemas/
COPY --chown=airflow:airflow keys/public_key.asc ./keys/public_key.asc

# copy sftp_directories.yaml so entrypoint.sh and directory_config.py
# can read it at container start to determine which directories to process
# and which need PGP secrets fetched from Secret Manager.
COPY --chown=airflow:airflow manifests/sftp_directories.yaml ./manifests/sftp_directories.yaml

# Directories and permissions
RUN mkdir -p \
        /opt/airflow/logs \
        /opt/airflow/plugins \
        /run/secrets \
        /tmp/gnupg \
    && chown -R airflow:airflow /opt/airflow /tmp/gnupg \
    && chmod 700 /tmp/gnupg \
    && chmod 700 /run/secrets

# Copy and make entrypoint executable
COPY --chown=airflow:airflow docker/entrypoint.sh ./docker/entrypoint.sh
RUN chmod +x ./docker/entrypoint.sh

# Environment
ENV AIRFLOW_HOME=/opt/airflow \
    PYTHONPATH=/opt/airflow \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # UPDATED: path where entrypoint.sh reads directory config
    SFTP_DIRECTORIES_YAML=/opt/airflow/manifests/sftp_directories.yaml \
    # UPDATED: SFTP_REMOTE_DIR removed — paths now come from sftp_directories.yaml
    # Airflow core settings
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__SECRETS__BACKEND=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend \
    AIRFLOW__LOGGING__LOGGING_LEVEL=INFO \
    AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN

USER airflow

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl --fail http://localhost:8080/health || exit 1

# Entrypoint + default command
ENTRYPOINT ["/opt/airflow/docker/entrypoint.sh"]
CMD ["airflow", "webserver"]