import os
import json
import sqlite3
from datetime import datetime
from dagster import asset, define_asset_job, ScheduleDefinition, AssetSelection, Definitions, AssetMaterialization, Output

# 從 test.py 匯入 run_crawler 函式（保留原有爬蟲邏輯）
from crawl_104 import run_crawler

# 新增：引入上傳函式
# import import_to_supabase

@asset
def crawl_jobs_asset_01(context):
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
def crawl_jobs_asset_02(context):
    """
    執行第二支爬蟲，爬取新的工作資料，並儲存為 JSON 檔案。
    """
    directory = "/opt/dagster/data/job_list_02"  # 與 crawl_job_02 中設定一致
    context.log.info("開始執行第二支爬蟲程式...")
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
        asset_key="crawl_jobs_asset_02",
        description="第二支爬蟲產生 JSON 檔案並讀取工作資料"
    )
    yield Output(job_data)


@asset
def write_jobs_to_db_01(context, crawl_jobs_asset_01):
    """
    從 crawl_jobs_asset 接收完整的工作資料，並將其寫入 SQLite 資料庫。
    你可依需求選擇寫入全部欄位或部分欄位，以下示範寫入原始資料中的主要欄位。
    """
    db_path = r"/opt/dagster/data/jobs.db"
    context.log.info(f"連線到資料庫：{db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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

    job_list = crawl_jobs_asset_01
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
job_first_crawler = define_asset_job(
    name="job_first_crawler",
    selection=AssetSelection.assets(crawl_jobs_asset_01, write_jobs_to_db_01),
)

job_second_crawler = define_asset_job(
    name="job_second_crawler",
    selection=AssetSelection.assets(crawl_jobs_asset_02),
)

# 定義job的排成
# 每 10 分鐘執行一次
schedule_first_crawler  = ScheduleDefinition(
    name="my_asset_schedule",
    job=job_first_crawler,
    cron_schedule="*/10 * * * *",  
    execution_timezone="Asia/Taipei",
)

# 排程第二支爬蟲：每天上午 9 點
schedule_second_crawler = ScheduleDefinition(
    name="schedule_second_crawler",
    job=job_second_crawler,
    cron_schedule="* 9 * * *",  # 每天 09:00
    execution_timezone="Asia/Taipei",
)

defs = Definitions(
    assets=[crawl_jobs_asset_01, write_jobs_to_db_01, crawl_jobs_asset_02],
    jobs=[job_first_crawler, job_second_crawler],
    schedules=[schedule_first_crawler, schedule_second_crawler ],
)
