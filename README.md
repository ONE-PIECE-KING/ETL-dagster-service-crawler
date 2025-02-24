進入資料夾
cd <存放資料的位址>

打開definitions.py，修改以下資料
1.地址：
script_path = r"D:\testAsset\test.py"
2.多久啟動一次：
cron_schedule="* * * * *",        # 每分鐘執行(可以自己需要的調整)

創建image：
docker build -t <image name>

創建container並執行：
docker run -p 3000:3000 --name <container name name> <image name>

打開localhost:3000，並點擊job，勾選哪個要執行，就可以開始爬蟲了

爬蟲程式結束之後，若想看內部檔案有兩種方法
在終端機輸入：
docker exec -it <container name> bash

1.看SQLite資料
sqlite3 /opt/dagster/data/jobs.db
.tables         #-- 檢查有哪些資料表
SELECT * FROM jobs;

2.查看json檔案
cd data
cd job_list
ls

