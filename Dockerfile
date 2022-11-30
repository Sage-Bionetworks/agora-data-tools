FROM python:3.9-slim-buster

WORKDIR /agora-data-tools

COPY . .

RUN pip install --no-cache-dir .
RUN apt-get install -y procps
