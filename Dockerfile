FROM python:3.11.15-trixie

RUN  apt-get update && \
  apt-get install -y procps && \
  apt-get install -y gcc && \
  apt-get install -y g++ && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /agora-data-tools

COPY Pipfile Pipfile.lock ./

RUN pip install --no-cache-dir --upgrade pip pipenv && \
    pipenv install --system --deploy

COPY . .
