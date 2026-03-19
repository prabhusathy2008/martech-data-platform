FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

ARG SOURCE_REPO=unknown

LABEL org.opencontainers.image.source="${SOURCE_REPO}"

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

RUN useradd --create-home --uid 10001 appuser
USER appuser

CMD ["python", "app/main.py"]