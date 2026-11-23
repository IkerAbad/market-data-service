FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN pip install .

RUN groupadd --gid 10001 mds \
    && useradd --uid 10001 --gid mds --create-home mds \
    && mkdir -p /data \
    && chown -R mds:mds /data /app

USER mds

ENV MDS_DATA_DIR=/data \
    MDS_HOST=0.0.0.0 \
    MDS_PORT=8000

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/v1/health', timeout=2)"

CMD ["python", "-m", "market_data_service"]
