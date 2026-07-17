# Python 3.12 — PyFlink 1.20's ceiling; pinned everywhere in this stack for one Python version.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY fixtures ./fixtures

# -u: unbuffered stdout so `docker compose logs` shows progress live.
CMD ["python", "-u", "producer.py"]
