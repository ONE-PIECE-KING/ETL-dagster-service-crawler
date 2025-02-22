FROM python:3.10-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1

RUN mkdir -p $DAGSTER_HOME

# 安裝 Chrome & ChromeDriver
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 3000

# 預設同時啟動 webserver 與 daemon
CMD ["/bin/bash", "-c", "dagster-webserver -w workspace.yaml --host 0.0.0.0 & dagster-daemon run"]
