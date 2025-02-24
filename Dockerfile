FROM python:3.10-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1

RUN mkdir -p $DAGSTER_HOME

# 安裝 Chrome、ChromeDriver 與 sqlite3
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster

COPY . .

# 如果 dagster.yaml 不存在，建立一個空檔案
RUN if [ ! -f "$DAGSTER_HOME/dagster.yaml" ]; then touch $DAGSTER_HOME/dagster.yaml; fi

RUN pip install --no-cache-dir -r requirements.txt

# 複製 entrypoint 腳本並賦予執行權限
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
