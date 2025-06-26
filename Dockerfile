FROM apache/airflow:2.8.4

USER root

# Install git and other necessary tools
RUN apt-get update && \
    apt-get install -y \
        git \
        openssh-client \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

USER airflow

# Configure git with default settings
RUN git config --global user.name "Airflow Bot" && \
    git config --global user.email "airflow@company.com" && \
    git config --global init.defaultBranch main && \
    git config --global safe.directory '*'

# Set git to use safe directory for any mounted volumes
RUN git config --global --add safe.directory /opt/airflow 