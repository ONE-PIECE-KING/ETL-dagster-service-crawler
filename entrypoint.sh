#!/bin/bash
set -e

echo "=== 開始資料庫 migration ==="

# 嘗試進行 migration
if ! dagster instance migrate; then
    echo "初次 migration 失敗，嘗試檢查是否有多個 migration head或 version table 重複..."
    
    # 嘗試合併多個 head
    HEADS=$(alembic heads -q)
    echo "目前的 heads：$HEADS"
    
    if [ -n "$HEADS" ]; then
        echo "執行 alembic merge 將多個 head 合併..."
        alembic merge -m "Merge multiple heads" $HEADS
    fi
    
    echo "嘗試 Stamp 資料庫至最新 revision..."
    # 將資料庫標記成最新版本，不進行 table 建立動作
    alembic stamp head
    
    echo "重新執行 migration..."
    dagster instance migrate
fi

echo "=== 資料庫 migration 完成 ==="

echo "=== 啟動 Dagster ==="
dagster-webserver -w workspace.yaml --host 0.0.0.0 &
dagster-daemon run
