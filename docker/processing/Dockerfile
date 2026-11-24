# Processing image: the data plane. Small, fast to build, no Airflow.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/src \
    MPLBACKEND=Agg

COPY requirements/processing.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app

# Source is bind-mounted at /src in docker-compose for live editing; the copy
# keeps the image usable on its own (e.g. `docker run ... python -m pytest`).
COPY src /src

CMD ["sleep", "infinity"]
