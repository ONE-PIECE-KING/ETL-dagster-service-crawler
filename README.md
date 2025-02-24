# Dagster 資產流程與爬蟲專案

本專案整合爬蟲與資料庫更新流程，並利用 Dagster 定時執行資產 job。請依照下列步驟進行設定與執行。

## 目錄結構

. ├── definitions.py # Dagster 資產與排程設定 ├── test.py # 爬蟲程式，內含完整爬取邏輯 ├── Dockerfile # Docker 映像檔建置腳本 ├── entrypoint.sh # Container 啟動腳本 ├── requirements.txt # Python 相依套件 └── data # 爬蟲產生的 JSON 檔案與資料庫存放位置 ├── job_list # JSON 檔案存放目錄 └── jobs.db # SQLite 資料庫


## 使用說明

### 1. 進入專案資料夾
請進入存放本專案的資料夾：

cd <存放資料的位址>

2. 修改設定
打開 definitions.py，修改下列內容：

地址設定：
如果有設定爬蟲程式的路徑（例如 script_path），請修改為你的實際路徑：
script_path = r"D:\testAsset\test.py"
排程設定：
修改 cron_schedule 參數，以設定多久執行一次，例如：
cron_schedule="* * * * *",  # 每分鐘執行（可依需求調整）

3. 建立 Docker 映像檔
在專案根目錄下執行下列指令建立映像檔，請將 <image name> 替換成你的映像檔名稱：
docker build -t <image name> .
4. 建立並執行 Container
使用下列指令建立並啟動 container，請將 <container name> 與 <image name> 替換成對應名稱：
docker run -p 3000:3000 --name <container name> <image name>
5. 使用 Dagster UI 觸發爬蟲
打開瀏覽器，前往 http://localhost:3000。
點擊左側選單中的 Job，選取你要執行的 Job（例如 my_asset_job）。
勾選並啟動後，系統即會開始執行爬蟲流程並更新資料庫。
查看資料
爬蟲程式執行完畢後，你可以透過下列方式查看產生的資料：

1. 使用終端機進入 Container
在主機上執行下列指令進入 container：
docker exec -it <container name> bash
查看 SQLite 資料
進入 container 後，執行：
sqlite3 /opt/dagster/data/jobs.db
進入 sqlite3 互動模式後，可執行：
.tables         -- 檢查有哪些資料表
SELECT * FROM jobs;
2. 查看 JSON 檔案
在 container 中，切換到 JSON 檔案存放目錄：
cd /opt/dagster/data/job_list
ls
你將看到爬蟲產生的各個 JSON 檔案。



