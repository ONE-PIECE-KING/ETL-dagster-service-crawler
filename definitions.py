import os
import json
import sqlite3
import re  # 記得要 import re 才能用 re.search
from datetime import datetime
from dagster import (
    asset,
    define_asset_job,
    ScheduleDefinition,
    AssetSelection,
    Definitions,
    AssetMaterialization,
    Output,
    Nothing,
)

from crawl_104 import run_crawler


def get_latest_job_details_file(directory):
    """
    從指定目錄中找出最新的 job_details JSON 檔案，
    檔名格式為 "job_details_YYYYMMDD_HHMMSS_%f.json"
    """
    os.makedirs(directory, exist_ok=True)
    
    try:
        files = [
            f for f in os.listdir(directory)
            if f.startswith("job_details_") and f.endswith(".json")
        ]
    except Exception as e:
        raise Exception(f"無法讀取目錄 {directory}，錯誤：{e}")
        
    if not files:
        return None

    def extract_timestamp(filename):
        match = re.search(r'job_details_(\d{8}_\d{6}(?:_\d{1,6})?)\.json', filename)
        if match:
            ts_str = match.group(1)
            try:
                return datetime.strptime(ts_str, "%Y%m%d_%H%M%S_%f")
            except ValueError:
                return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        return datetime.min

    files.sort(key=lambda f: extract_timestamp(f), reverse=True)
    return os.path.join(directory, files[0])


# ----------------------------------------------------------------
# 第一支爬蟲：產生一個名為 crawl_jobs_asset_01 的資產
# 回傳 job_data (list)，下游可以直接拿到資料
# ----------------------------------------------------------------
@asset
def crawl_jobs_asset_01(context):
    """
    執行第一支爬蟲並回傳工作資料 (list)。
    """
    directory = "/opt/dagster/data/job_list"
    context.log.info("開始執行爬蟲程式(第一支)...")
    file_path, job_list_crawled = run_crawler(["區塊鏈工程師"], output_directory=directory)
    context.log.info(f"爬蟲完成，檔案儲存於 {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            job_data = json.load(f)
        context.log.info(f"成功讀取 {len(job_data)} 筆工作資料 (第一支)")
    except Exception as e:
        context.log.error(f"讀取 {file_path} 時發生錯誤：{e}")
        job_data = []

    # 這裡也可以加上 AssetMaterialization 作為記錄
    yield AssetMaterialization(
        asset_key="crawl_jobs_asset_01_materialization",
        description="第一支爬蟲產生 JSON 檔案並讀取工作資料"
    )

    # 回傳資料給下游使用
    yield Output(job_data)


# ----------------------------------------------------------------
# 第二支爬蟲：產生一個名為 crawl_jobs_asset_02 的資產
# 回傳 job_data (list)，下游可以直接拿到資料
# ----------------------------------------------------------------
@asset
def crawl_jobs_asset_02(context):
    """
    執行第二支爬蟲並回傳工作資料 (list)。
    """
    directory = "/opt/dagster/data/job_list_02"
    context.log.info("開始執行爬蟲程式(第二支)...")
    file_path, job_list_crawled = run_crawler(["AI工程師", "演算法工程師"], output_directory=directory)
    context.log.info(f"第二支爬蟲完成，檔案儲存於 {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            job_data = json.load(f)
        context.log.info(f"成功讀取 {len(job_data)} 筆工作資料 (第二支)")
    except Exception as e:
        context.log.error(f"讀取 {file_path} 時發生錯誤：{e}")
        job_data = []

    yield AssetMaterialization(
        asset_key="crawl_jobs_asset_02_materialization",
        description="第二支爬蟲產生 JSON 檔案並讀取工作資料"
    )

    yield Output(job_data)


# ----------------------------------------------------------------
# 第一個展示資產：顯示 crawl_jobs_asset_01 產生的資料
# 注意：函式參數名與上游資產函式名相同 => crawl_jobs_asset_01
# ----------------------------------------------------------------
@asset
def display_latest_json_asset_01(context, crawl_jobs_asset_01):
    """
    下游資產，會拿到上游回傳的 job_data (list) 作為參數 crawl_jobs_asset_01。
    同時也可以自行去讀檔案 ("/opt/dagster/data/job_list") 進行顯示。
    """
    # 如果你只是想依賴順序，而不需要 job_data，可以改成 : Nothing
    # 但目前我們寫成要拿到 job_data
    context.log.info(f"接收到上游的工作資料 {len(crawl_jobs_asset_01)} 筆")

    directory = "/opt/dagster/data/job_list"
    file_path = get_latest_job_details_file(directory)
    
    if not file_path:
        context.log.warning("找不到最新的 JSON 檔案")
        return {}
    else:
        context.log.info(f"讀取最新 JSON 檔案：{file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            context.log.info(f"成功讀取 {len(data)} 筆資料 (從檔案)")
        except Exception as e:
            context.log.error(f"讀取 JSON 檔案時發生錯誤：{e}")
            data = {}

        yield AssetMaterialization(
            asset_key="display_latest_json_asset_01",
            description=f"顯示最新產生的 JSON 檔案：{file_path}"
        )
        yield Output(data)


# ----------------------------------------------------------------
# 第二個展示資產：顯示 crawl_jobs_asset_02 產生的資料
# 注意：函式參數名與上游資產函式名相同 => crawl_jobs_asset_02
# ----------------------------------------------------------------
@asset
def display_latest_json_asset_02(context, crawl_jobs_asset_02):
    """
    下游資產，會拿到上游回傳的 job_data (list) 作為參數 crawl_jobs_asset_02。
    同時也可以自行去讀檔案 ("/opt/dagster/data/job_list_02") 進行顯示。
    """
    context.log.info(f"接收到上游的工作資料 {len(crawl_jobs_asset_02)} 筆")

    directory = "/opt/dagster/data/job_list_02"  # or 依需求調整
    file_path = get_latest_job_details_file(directory)
    
    if not file_path:
        context.log.warning("找不到最新的 JSON 檔案")
        return {}
    else:
        context.log.info(f"讀取最新 JSON 檔案：{file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            context.log.info(f"成功讀取 {len(data)} 筆資料 (從檔案)")
        except Exception as e:
            context.log.error(f"讀取 JSON 檔案時發生錯誤：{e}")
            data = {}

        yield AssetMaterialization(
            asset_key="display_latest_json_asset_02",
            description=f"顯示最新產生的 JSON 檔案：{file_path}"
        )
        yield Output(data)


# ----------------------------------------------------------------
# 定義兩個 Job，分別只執行第一組 or 第二組爬蟲與展示
# ----------------------------------------------------------------
job_first_crawler = define_asset_job(
    name="job_first_crawler",
    selection=AssetSelection.assets(
        crawl_jobs_asset_01, 
        display_latest_json_asset_01
    ),
)

job_second_crawler = define_asset_job(
    name="job_second_crawler",
    selection=AssetSelection.assets(
        crawl_jobs_asset_02, 
        display_latest_json_asset_02
    ),
)


# ----------------------------------------------------------------
# 定義兩個排程
# ----------------------------------------------------------------
schedule_first_crawler = ScheduleDefinition(
    name="my_asset_schedule",
    job=job_first_crawler,
    cron_schedule="*/10 * * * *",  
    execution_timezone="Asia/Taipei",
)

schedule_second_crawler = ScheduleDefinition(
    name="schedule_second_crawler",
    job=job_second_crawler,
    cron_schedule="0 9 * * *",  # 每天 09:00
    execution_timezone="Asia/Taipei",
)

# ----------------------------------------------------------------
# 組合成最終的 Definitions
# ----------------------------------------------------------------
defs = Definitions(
    assets=[
        crawl_jobs_asset_01,
        crawl_jobs_asset_02,
        display_latest_json_asset_01,
        display_latest_json_asset_02,
    ],
    jobs=[job_first_crawler, job_second_crawler],
    schedules=[schedule_first_crawler, schedule_second_crawler],
)
