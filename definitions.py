import os
import re
import json
import sqlite3
from datetime import datetime
from dagster import asset, define_asset_job, ScheduleDefinition, AssetSelection, Definitions, AssetMaterialization, Output

def get_latest_job_details_file(directory):
    """
    從指定目錄中找出最新的 job_details JSON 檔案，
    檔名格式為 "job_details_YYYYMMDD_HHMMSS.json"
    """
    # 自動建立目錄，避免因目錄不存在而拋出例外
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
        match = re.search(r'job_details_(\d{8}_\d{6})\.json', filename)
        if match:
            return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
        return datetime.min

    # 依照檔案中的 timestamp 由新到舊排序，取最新一個
    files.sort(key=lambda f: extract_timestamp(f), reverse=True)
    return os.path.join(directory, files[0])

# Asset 1：讀取爬蟲產生的 JSON 資料
@asset
def crawl_jobs_asset(context):
    directory = "/opt/dagster/data/job_list"
    latest_file = get_latest_job_details_file(directory)
    if not latest_file:
        context.log.error(f"在目錄 {directory} 找不到符合規則的 job_details JSON 檔案")
        job_data = []
    else:
        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                job_data = json.load(f)
            context.log.info(f"成功讀取 {len(job_data)} 筆工作資料，來源檔案：{latest_file}")
        except Exception as e:
            context.log.error(f"讀取 {latest_file} 時發生錯誤：{e}")
            job_data = []

    yield AssetMaterialization(
        asset_key="crawl_jobs_asset",
        description="從 job_list 目錄讀取最新的工作資料 JSON"
    )
    yield Output(job_data)

# Asset 2：將爬蟲資料寫入 SQLite 資料庫
@asset
def write_jobs_to_db(context, crawl_jobs_asset):
    db_path = r"/opt/dagster/data/jobs.db"
    context.log.info(f"連線到資料庫：{db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 建立資料表，包含所有欄位（依照爬蟲資料內容）
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

    job_list = crawl_jobs_asset  # 從上一個 asset 取得資料
    if job_list:
        for job in job_list:
            # 將「應徵分析」巢狀資料轉換為 JSON 字串儲存
            analysis_json = json.dumps(job.get("應徵分析", {}), ensure_ascii=False)
            cursor.execute('''
                INSERT INTO jobs (
                    職缺名稱, 職缺網址, 公司名稱, 更新日期,
                    積極徵才, 應徵人數, 工作內容, 職務類別,
                    工作待遇, 工作性質, 上班地點, 管理責任,
                    出差外派, 上班時段, 休假制度, 可上班日,
                    需求人數, 工作經歷, 學歷要求, 科系要求,
                    語文條件, 擅長工具, 工作技能, 具備證照,
                    其他條件, 法定福利, 其他福利, 原始福利,
                    聯絡資訊, 應徵分析
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
                analysis_json 
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

# 定義整體資產 Job 與排程
my_asset_job = define_asset_job(
    name="my_asset_job",
    selection=AssetSelection.assets(crawl_jobs_asset, write_jobs_to_db),
)

my_asset_schedule = ScheduleDefinition(
    name="my_asset_schedule",
    job=my_asset_job,
    cron_schedule="*/40 * * * *",  # 每40分鐘執行一次，可根據需求調整
    execution_timezone="Asia/Taipei",
)

defs = Definitions(
    assets=[crawl_jobs_asset, write_jobs_to_db],
    schedules=[my_asset_schedule],
)
