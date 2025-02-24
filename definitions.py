import os
import re
import json
import sqlite3
from datetime import datetime
from dagster import asset, define_asset_job, ScheduleDefinition, AssetSelection, Definitions, AssetMaterialization, Output

# 從 test.py 匯入 run_crawler 函數（保留原有爬蟲邏輯）
from test import run_crawler

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

@asset
def crawl_jobs_asset(context):
    """
    先執行爬蟲程式產生 JSON 檔案，並讀取該檔案回傳工作資料。
    此處完整保留 test.py 中的所有欄位資料。
    """
    directory = "/opt/dagster/data/job_list"
    context.log.info("開始執行爬蟲程式...")
    file_path, job_list_crawled = run_crawler(["區塊鏈工程師"], output_directory=directory)
    context.log.info(f"爬蟲完成，檔案儲存於 {file_path}")
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            job_data = json.load(f)
        context.log.info(f"成功讀取 {len(job_data)} 筆工作資料")
    except Exception as e:
        context.log.error(f"讀取 {file_path} 時發生錯誤：{e}")
        job_data = []

    yield AssetMaterialization(
        asset_key="crawl_jobs_asset",
        description="爬蟲產生 JSON 檔案並讀取工作資料"
    )
    yield Output(job_data)

@asset
def write_jobs_to_db(context, crawl_jobs_asset):
    """
    從 crawl_jobs_asset 接收完整的工作資料，並將其寫入 SQLite 資料庫。
    你可依需求選擇寫入全部欄位或部分欄位，以下示範寫入原始資料中的主要欄位。
    """
    db_path = r"/opt/dagster/data/jobs.db"
    context.log.info(f"連線到資料庫：{db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 建立資料表，這裡示範寫入全部或部分欄位（你可自行擴充）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            職缺名稱 TEXT,
            職缺網址 TEXT,
            公司名稱 TEXT,
            更新日期 TEXT,
            積極徵才 TEXT,
            應徵人數 TEXT,
            工作內容 TEXT,
            職務類別 TEXT,
            工作待遇 TEXT,
            工作性質 TEXT,
            上班地點 TEXT,
            管理責任 TEXT,
            出差外派 TEXT,
            上班時段 TEXT,
            休假制度 TEXT,
            可上班日 TEXT,
            需求人數 TEXT,
            工作經歷 TEXT,
            學歷要求 TEXT,
            科系要求 TEXT,
            語文條件 TEXT,
            擅長工具 TEXT,
            工作技能 TEXT,
            具備證照 TEXT,
            其他條件 TEXT,
            法定福利 TEXT,
            其他福利 TEXT,
            原始福利 TEXT,
            聯絡資訊 TEXT,
            應徵分析 TEXT
        )
    ''')
    conn.commit()

    job_list = crawl_jobs_asset
    if job_list:
        for job in job_list:
            cursor.execute('''
                INSERT INTO jobs (
                    職缺名稱, 職缺網址, 公司名稱, 更新日期, 積極徵才, 應徵人數, 工作內容, 職務類別,
                    工作待遇, 工作性質, 上班地點, 管理責任, 出差外派, 上班時段, 休假制度, 可上班日,
                    需求人數, 工作經歷, 學歷要求, 科系要求, 語文條件, 擅長工具, 工作技能, 具備證照,
                    其他條件, 法定福利, 其他福利, 原始福利, 聯絡資訊, 應徵分析
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (
                job.get("職缺名稱"),
                job.get("職缺網址"),
                job.get("公司名稱"),
                job.get("更新日期"),
                job.get("積極徵才"),
                job.get("應徵人數"),
                job.get("工作內容"),
                job.get("職務類別"),
                job.get("工作待遇"),
                job.get("工作性質"),
                job.get("上班地點"),
                job.get("管理責任"),
                job.get("出差外派"),
                job.get("上班時段"),
                job.get("休假制度"),
                job.get("可上班日"),
                job.get("需求人數"),
                job.get("工作經歷"),
                job.get("學歷要求"),
                job.get("科系要求"),
                job.get("語文條件"),
                job.get("擅長工具"),
                job.get("工作技能"),
                job.get("具備證照"),
                job.get("其他條件"),
                job.get("法定福利"),
                job.get("其他福利"),
                job.get("原始福利"),
                job.get("聯絡資訊"),
                json.dumps(job.get("應徵分析", {}), ensure_ascii=False)
            ))
        conn.commit()
        context.log.info("資料已成功寫入資料庫")
    else:
        context.log.warning("無工作資料，資料庫未更新")
    conn.close()

    yield AssetMaterialization(
        asset_key="write_jobs_to_db",
        description="將最新工作資料寫入 SQLite 資料庫完成"
    )
    yield Output("資料庫更新完成")

# 定義整體資產 job 與排程
my_asset_job = define_asset_job(
    name="my_asset_job",
    selection=AssetSelection.assets(crawl_jobs_asset, write_jobs_to_db),
)

my_asset_schedule = ScheduleDefinition(
    name="my_asset_schedule",
    job=my_asset_job,
    cron_schedule="*/10 * * * *",  # 每 10 分鐘執行一次
    execution_timezone="Asia/Taipei",
)

defs = Definitions(
    assets=[crawl_jobs_asset, write_jobs_to_db],
    schedules=[my_asset_schedule],
)
