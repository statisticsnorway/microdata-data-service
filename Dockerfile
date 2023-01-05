# Export Poetry Packages
FROM ubuntu:22.04 as builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.1.14 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

# Prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# Install python 3.10
RUN apt-get update \
    &&  apt-get install -y --no-install-recommends \
    python3-pip python3-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

# Install tools
RUN apt-get update \
    && apt-get install -y  --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    python3-distutils \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Install poetry and export dependencies to requirements yaml
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL https://install.python-poetry.org | python3 -

#Set application version in pyproject.toml, use zero if not set
ARG BUILD_NUMBER=0
RUN poetryVersion=$(poetry version -s); buildNumber=${BUILD_NUMBER}; newVersion=$(echo $poetryVersion | sed "s/[[:digit:]]\{1,\}$/$buildNumber/"); poetry version $newVersion

RUN poetry export > requirements.txt

# Production image
FROM python:3.10-slim

WORKDIR /app
COPY data_service data_service
COPY static static
COPY application.py application.py
#To use application version in logs
COPY --from=builder /app/pyproject.toml pyproject.toml
COPY --from=builder /app/requirements.txt requirements.txt

RUN pip install -r requirements.txt

#the output is sent straight to terminal without being first buffered
ENV PYTHONUNBUFFERED 1

CMD [ "python", "application.py"]
