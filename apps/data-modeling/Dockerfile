FROM python:3.11-slim

RUN apt-get update
RUN apt-get install -y --no-install-recommends git
RUN rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN chmod +x /app/app/run_dbt.sh

ARG SOURCE_REPO=unknown
LABEL org.opencontainers.image.source="${SOURCE_REPO}"

RUN useradd --create-home --uid 10001 appuser
RUN chown -R appuser:appuser /app
USER appuser

CMD ["/app/app/run_dbt.sh"]