# definitions.py
import subprocess
import sys
from dagster import asset, define_asset_job, ScheduleDefinition, AssetSelection, Definitions

@asset
def call_time_script_asset(context):
    python_exe = sys.executable
    script_path = r"/opt/dagster/test.py"  # 在容器中的路徑

    context.log.info(f"使用的 python 解譯器: {python_exe}")
    context.log.info(f"執行的腳本: {script_path}")

    result = subprocess.run(
        [python_exe, script_path],
        capture_output=True,
        text=True,
        check=False
    )
    context.log.info(f"stdout: {result.stdout}")
    context.log.info(f"stderr: {result.stderr}")
    if result.returncode != 0:
        context.log.error(f"失敗 return code = {result.returncode}")
    else:
        context.log.info("test.py 執行成功")

my_asset_job = define_asset_job(
    name="my_asset_job",
    selection=AssetSelection.assets(call_time_script_asset),
)

my_asset_schedule = ScheduleDefinition(
    name="my_asset_schedule",
    job=my_asset_job,
    cron_schedule="*/2 * * * *",        # 每分鐘執行
    execution_timezone="Asia/Taipei", # 台灣時間
)

defs = Definitions(
    assets=[call_time_script_asset],
    schedules=[my_asset_schedule],
)
