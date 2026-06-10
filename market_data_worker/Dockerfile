FROM public.ecr.aws/docker/library/python:3.12-slim

WORKDIR /app
COPY worker.py /app/worker.py

CMD ["python", "/app/worker.py"]
