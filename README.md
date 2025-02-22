# 進入資料夾
cd <存放資料的位址>

# 打開definitions.py，修改以下資料
# 1.地址
script_path = r"D:\testAsset\test.py"
# 2.多久啟動一次
cron_schedule="* * * * *",        # 每分鐘執行(可以自己需要的調整)

# 創建image
docker build -t my-dagster-asset:latest .

# 創建container並執行
docker run -p 3000:3000 --name my_dagster_container my-dagster-asset:latest

# 打開localhost:3000，並點擊job就可以開始爬蟲了
