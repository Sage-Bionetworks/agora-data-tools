FROM python:3.9-slim-buster

RUN  apt-get update && \
  apt-get install -y procps && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /agora-data-tools

COPY . .

RUN pip install --no-cache-dir .
