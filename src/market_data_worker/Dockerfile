# Market-data worker runtime image
FROM public.ecr.aws/docker/library/python:3.12-slim

ARG GITHUB_USER
ARG GITHUB_TOKEN

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir -r /app/requirements.txt

COPY worker.py /app/worker.py
COPY publishers /app/publishers

CMD ["python", "-u", "/app/worker.py"]
