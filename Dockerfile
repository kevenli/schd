FROM python:3.9-slim-buster AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

WORKDIR /app_src
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.9-slim-buster
ENV TZ=Asia/Shanghai
WORKDIR /app
COPY rafdb /app/rafdb
COPY scripts /app/scripts
COPY conf /app/conf
RUN chmod +x scripts/*.sh
COPY --from=builder /venv /venv
ENV PATH="/venv/bin:$PATH"
