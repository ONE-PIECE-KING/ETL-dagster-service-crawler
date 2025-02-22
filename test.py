import time
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime
from fake_useragent import UserAgent
import os
import logging

def setup_logging(log_dir='logs'):
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f'job_crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s', 
                                                datefmt='%Y-%m-%d %H:%M:%S'))
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s',
                                                    datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return log_filename

keyword_list = ['區塊鏈工程師']

def is_similar_rgb(rgb_str, target_rgb):
    rgb_values = [int(x) for x in rgb_str.replace("rgb(", "").replace(")", "").split(",")]
    tolerance = 5
    return all(abs(a - b) <= tolerance for a, b in zip(rgb_values, target_rgb))

# 若需要擷取其他資訊，可參考以下函數（目前暫不啟用）
def extract_age_distribution(details_div):
    age_distribution = {}
    data_lines = details_div.find_elements(By.CSS_SELECTOR, 'div')
    for line in data_lines:
        parts = line.text.split('\n')
        if len(parts) == 2:
            age_range, percentage = parts[0], parts[1]
            age_distribution[age_range] = percentage
    return age_distribution

def extract_experience_distribution(details_div):
    experience_distribution = {}
    data_lines = details_div.find_elements(By.CSS_SELECTOR, 'div')
    for line in data_lines:
        parts = line.text.split('\n')
        if len(parts) == 2:
            experience_range, percentage = parts[0], parts[1]
            experience_distribution[experience_range] = percentage
    return experience_distribution

log_file = setup_logging()
logging.info(f"日誌檔案已建立：{log_file}")

# 設定 Chrome 選項與 UserAgent
chrome_options = Options()
ua = UserAgent()
chrome_options.add_argument(f'user-agent={ua.random}')
chrome_options.add_argument('--start-maximized')
chrome_options.add_argument('--remote-debugging-port=9222')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
# 若要除錯可暫時移除 headless
chrome_options.add_argument("--headless")
chrome_options.add_argument('--disable-dev-shm-usage')


# 根據環境設定 Chrome 路徑
if os.path.exists("/usr/bin/chromium"):  # Docker 環境
    chrome_options.binary_location = "/usr/bin/chromium"
    service = Service("/usr/bin/chromedriver")
else:  # Windows 本地環境
    service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
# 定義目標元素的 CSS 選擇器
target_selector = 'div.job-summary'
## 測試用
max_scrolls = 10000
max_scrolls = 1
scrolls = 1
job_list = []
com_list = []
def save_to_json(job_data_list, filename=None, mode='w', directory='default_directory'):
    """
    將職缺資料存成 JSON 檔案
    :param job_data_list: 職缺資料列表
    :param filename: 自訂檔名，預設為當前日期時間
    :param mode: 檔案寫入模式，預設為覆蓋 'w'，可選 'a' 為附加
    :param directory: 存放位置，預設為 'default_directory'
    """
    # 如果未提供檔名，使用當前日期時間
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"job_details_{timestamp}.json"
    # 確保檔名以 .json 結尾
    if not filename.endswith('.json'):
        filename += '.json'
    # 確保目錄存在
    if not os.path.exists(directory):
        os.makedirs(directory)
    # 完整的檔案路徑
    file_path = os.path.join(directory, filename)
    try:
        # 檢查檔案是否已存在且模式為附加
        if mode == 'a' and os.path.exists(file_path):
            # 讀取現有的 JSON 資料
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            # 合併新舊資料
            job_data_list = existing_data + job_data_list
        # 使用 UTF-8 編碼寫入 JSON 檔案
        with open(file_path, mode, encoding='utf-8') as f:
            json.dump(job_data_list, f, ensure_ascii=False, indent=4)
        logging.info(f"資料已成功儲存至 {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"儲存 JSON 檔案時發生錯誤: {e}")
        return None
def crawl_jobs(keyword_list, max_errors=3):
    crawler_error = 0
    for keyword in keyword_list:
        try:
            # 爬蟲邏輯
            url = f"https://www.104.com.tw/jobs/search/?keyword={keyword}"
            driver.get(url)
            time.sleep(5)
            # 處理職缺
            process_jobs(driver)
        except Exception as e:
            crawler_error += 1
            logging.error(f"爬蟲 {keyword} 發生錯誤: {e}")
            if crawler_error >= max_errors:
                logging.warning(f"已達到最大錯誤次數 {max_errors}，停止爬蟲")
                break
# for i in keyword_list:
#     # 設定目標 URL
#     url = f"https://www.104.com.tw/jobs/search/?keyword={i}"
#     driver.get(url)
#     time.sleep(5)
#     while scrolls < max_scrolls:
def process_jobs(driver):
    global scrolls
    scrolls = 0
    crawler_error = 0
    old_scrolls = 0
    while scrolls < max_scrolls:
        try:
            logging.info(f"正在處理第 {scrolls + 1} 頁...")
            # 獲取當前頁面的職缺
            current_jobs = driver.find_elements(By.CSS_SELECTOR, target_selector)
            logging.info(f"當前頁面職缺數量: {len(current_jobs)}")
            # 處理當前頁面的每個職缺 並且old_scrolls已經爬過的就不會計算
            # for job in current_jobs[old_scrolls:]:
            ## 測試用
            for job in current_jobs[:1]:
                try:
                    # 獲取職缺名稱和連結
                    title_element = job.find_element(By.CSS_SELECTOR, 'h2 a.info-job__text')
                    job_url = title_element.get_attribute('href')
                    job_name = title_element.get_attribute('title')
                    # 獲取公司名稱
                    company_element = job.find_element(By.CSS_SELECTOR, 'a[data-gtm-joblist="職缺-公司名稱"]')
                    company = company_element.text.strip()
                    company_url = company_element.get_attribute('href')
                    # 開啟新分頁獲取詳細資訊
                    driver.execute_script(f"window.open('{job_url}', '_blank')")
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(3)
                    # 處理詳細頁面的資訊
                    try:
                        logging.info(f"職缺名稱: {job_name}")
                        logging.info(f"職缺網址: {job_url}")
                        logging.info(f"公司名稱: {company}")
                        # 獲取更新日期，使用 title 屬性來獲取完整日期（包含年份）
                        update_date_element = driver.find_element(By.CSS_SELECTOR, 'span.text-gray-darker[title*="更新"]')
                        update_date = update_date_element.get_attribute('title')  # 獲取完整的 title 內容
                        update_date = update_date.replace("更新", "").strip()  # 移除 "更新" 文字
                        logging.info(f"更新日期: {update_date}")
                        # 檢查是否為積極徵才中（可能不存在）
                        try:
                            actively_hiring = driver.find_element(By.CSS_SELECTOR, 'div.actively-hiring-tag').text.strip()
                            actively_hiring = "是" if actively_hiring == "積極徵才中" else "否"
                        except:
                            actively_hiring = "否"
                        # 獲取應徵人數
                        try:
                            applicants = driver.find_element(By.CSS_SELECTOR, 'a.d-flex.align-items-center.font-weight-bold').text.strip()
                            # 提取數字範圍（例如："應徵人數 0~5 人" -> "0~5"）
                            applicants = applicants.replace("應徵人數", "").replace("人", "").strip()
                            # print("應徵人數:", applicants)
                        except Exception as e:
                            applicants = "N/A"
                            logging.info(f"獲取應徵人數時發生錯誤: {e}")
                            # print("無法獲取應徵人數")
                        # 獲取工作內容
                        job_description = driver.find_element(By.CSS_SELECTOR, 'p.job-description__content').text.strip()
                        # 獲取職務類別
                        job_categories = driver.find_elements(By.CSS_SELECTOR, 'div.category-item u')
                        job_category = '、'.join([cat.text for cat in job_categories])
                        # 獲取工作待遇
                        salary = driver.find_element(By.CSS_SELECTOR, 'p.text-primary.font-weight-bold').text.strip()
                        # 獲取工作性質
                        job_type = driver.find_element(By.CSS_SELECTOR, 'div.list-row:nth-child(4) div.list-row__data').text.strip()
                        # 獲取上班地點
                        location = driver.find_element(By.CSS_SELECTOR, 'div.job-address span').text.strip()
                        # 獲取管理責任
                        management_elements = driver.find_elements(By.CSS_SELECTOR, 'div.list-row')
                        management = ""
                        for element in management_elements:
                            try:
                                title_text = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title_text == "管理責任":
                                    management = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取管理責任時發生錯誤: {e}")
                                continue
                        # 獲取出差外派
                        business_trip = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "出差外派":
                                    business_trip = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取出差外派時發生錯誤: {e}")
                                continue
                        # 獲取上班時段
                        work_time = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "上班時段":
                                    work_time = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取上班時段時發生錯誤: {e}")
                                continue
                        # 獲取休假制度
                        vacation = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "休假制度":
                                    vacation = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取休假制度時發生錯誤: {e}")
                                continue
                        # 獲取可上班日
                        start_work = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "可上班日":
                                    start_work = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取可上班日時發生錯誤: {e}")
                                continue
                        # 獲取需求人數
                        headcount = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "需求人數":
                                    headcount = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取需求人數時發生錯誤: {e}")
                                continue
                        # 獲取工作經歷
                        work_exp = ""
                        work_exp_elements = driver.find_elements(By.CSS_SELECTOR, 'div.list-row')
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "工作經歷":
                                    work_exp = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取工作經歷時發生錯誤: {e}")
                                continue
                        # 獲取學歷要求
                        education = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "學歷要求":
                                    education = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取學歷要求時發生錯誤: {e}")
                                continue
                        # 獲取科系要求
                        major = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "科系要求":
                                    major = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取科系要求時發生錯誤: {e}")
                                continue
                        # 獲取語文條件
                        language = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "語文條件":
                                    language = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取語文條件時發生錯誤: {e}")
                                continue
                        # 獲取擅長工具
                        tools = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "擅長工具":
                                    tools_elements = element.find_elements(By.CSS_SELECTOR, 'div.list-row__data u')
                                    tools = '、'.join([tool.text for tool in tools_elements])
                                    break
                            except Exception as e:
                                logging.error(f"獲取擅長工具時發生錯誤: {e}")
                                continue
                        # 獲取工作技能
                        skills = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "工作技能":
                                    skills_elements = element.find_elements(By.CSS_SELECTOR, 'div.list-row__data u')
                                    skills = '、'.join([skill.text for skill in skills_elements])
                                    break
                            except Exception as e:
                                logging.error(f"獲取工作技能時發生錯誤: {e}")
                                continue
                        # 獲取具備證照
                        certificates = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "具備證照":
                                    cert_elements = element.find_elements(By.CSS_SELECTOR, 'div.list-row__data u')
                                    certificates = '、'.join([cert.text for cert in cert_elements])
                                    break
                            except Exception as e:
                                logging.error(f"獲取具備證照時發生錯誤: {e}")
                                continue
                        # 獲取其他條件
                        other_requirements = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "其他條件":
                                    other_requirements = element.find_element(By.CSS_SELECTOR, 'div.list-row__data p.r3').text.strip()
                                    break
                            except Exception as e:
                                logging.error(f"獲取其他條件時發生錯誤: {e}")
                                continue
                        # 獲取福利制度
                        try:
                            # 法定項目
                            legal_benefits = []
                            legal_elements = driver.find_elements(By.CSS_SELECTOR, 'div.benefits-labels:nth-child(3) span.tag--text a')
                            legal_benefits = [item.text.strip() for item in legal_elements]
                            legal_benefits_str = '、'.join(legal_benefits)
                            # print("法定項目:", legal_benefits_str)
                            
                            # 其他福利
                            other_benefits = []
                            other_elements = driver.find_elements(By.CSS_SELECTOR, 'div.benefits-labels:nth-child(5) span.tag--text a')
                            other_benefits = [item.text.strip() for item in other_elements]
                            other_benefits_str = '、'.join(other_benefits)
                            # print("其他福利:", other_benefits_str)
                            
                            # 未整理的福利說明
                            raw_benefits = ""
                            benefits_description = driver.find_element(By.CSS_SELECTOR, 'div.benefits-description p.r3').text.strip()
                            raw_benefits = benefits_description
                            # print("未整理的福利說明:", raw_benefits)
                            
                        except Exception as e:
                            logging.error(f"獲取福利制度時發生錯誤: {e}")
                            legal_benefits_str = ""
                            other_benefits_str = ""
                            raw_benefits = ""
                        
                        # 獲取聯絡方式
                        try:
                            contact_info = []
                            contact_elements = driver.find_elements(By.CSS_SELECTOR, 'div.job-contact-table div.job-contact-table__data')
                            contact_info = [element.text.strip() for element in contact_elements]
                            contact_info_str = '\n'.join(contact_info)
                            # print("聯絡方式:", contact_info_str)
                        except Exception as e:
                            logging.error(f"獲取聯絡方式時發生錯誤: {e}")
                            contact_info_str = ""     

                        try:
                            # 開啟應徵分頁獲取詳細資訊
                            # 從原始工作頁面 URL 提取工作代碼
                            apply_code = job_url.split('/')[-1].split('?')[0]
                            # 構建應徵分析頁面的 URL
                            apply_analysis_url = f"https://www.104.com.tw/jobs/apply/analysis/{apply_code}"
                            driver.execute_script(f"window.open('{apply_analysis_url}', '_blank')")
                            driver.switch_to.window(driver.window_handles[-1])
                            # 建立字典存儲資訊
                            job_info = {}
                            # 抓取教育程度分布
                            try:
                                apply_education = {}
                                education_elements = driver.find_elements(By.CSS_SELECTOR, "div.legend__text")
                                education_values = driver.find_elements(By.CSS_SELECTOR, "div.legend__value") 
                                for i in range(len(education_elements)):
                                    apply_education[education_elements[i].text] = education_values[i].text
                                job_info["學歷"] = apply_education
                            except Exception as e:
                                logging.error(f"獲取聯絡方式時發生錯誤: {e}")
                            print(job_info)
                            # 抓取性別分布
                            gender = {}
                            gender_elements = driver.find_elements(By.CSS_SELECTOR, ".stack-bar__text__block")
                            for element in gender_elements[:2]:
                                style = element.get_attribute("style")
                                rgb_value = style[style.find("rgb"):style.find(")") + 1]
                                gender_text = element.find_element(By.CSS_SELECTOR, "div").text
                                # 定義目標RGB值
                                male_rgb = [78, 145, 255]    # 藍色
                                female_rgb = [255, 144, 199]  # 粉色
                                if is_similar_rgb(rgb_value, male_rgb):
                                    gender["男性"] = gender_text
                                elif is_similar_rgb(rgb_value, female_rgb):
                                    gender["女性"] = gender_text
                            job_info["性別"] = gender
                            print(job_info)
                            # 抓取語言能力
                            # 選取div.chart-container__body的第5個是下下之策
                            language_container = driver.find_elements(By.CSS_SELECTOR, "div.chart-container__body")[5]
                            print(language_container)
                            # 初始化語言能力字典
                            language_skills = {}
                            
                            # 找出所有語言項目
                            language_items = language_container.find_elements(By.XPATH, ".//div[contains(@class, 'mb-4')]")
                            print(language_items)
                            for language_item in language_items:
                                # 提取語言名稱
                                language_name = language_item.find_element(By.XPATH, ".//span[contains(@class, 'text-truncate')]").text
                                print(language_name)
                                # 找出該語言的技能等級和百分比
                                skill_bars = language_item.find_elements(By.XPATH, ".//div[contains(@class, 'stack-bar__text__block')]")
                                print(skill_bars)
                                # 建立該語言的技能描述
                                language_description = []
                                # 圖例映射
                                legend_map = {
                                    "rgb(255, 231, 217)": "不會",
                                    "rgb(255, 213, 189)": "略懂",
                                    "rgb(255, 195, 161)": "中等",
                                    "rgb(204, 156, 129)": "精通"
                                }
                                for bar in skill_bars:
                                    try:
                                        percentage = bar.text
                                        # 獲取背景顏色
                                        background_color = bar.get_attribute('style').split('background:')[1].split(';')[0].strip()
                                        skill_level = legend_map.get(background_color, "未知")
                                        language_description.append(f"{skill_level}{percentage}")
                                    except Exception as level_error:
                                        print(f"提取{language_name}技能等級時出錯: {level_error}")
                                # 將語言技能加入字典
                                language_skills[language_name] = ','.join(language_description)
                            job_info['語言能力'] = language_skills
                            # 主要處理邏輯
                            # 定位所有的圖表容器
                            chart_containers = driver.find_elements(By.CSS_SELECTOR, 'div.chart-container.d-flex.flex-column.bg-white.overflow-hidden.horizontal-bar-chart')
                            # 欄位名稱列表
                            fields = {
                                '年齡': extract_age_distribution,
                                '工作經驗': extract_experience_distribution,
                                '科系': extract_experience_distribution,  # 可以重複使用
                                '技能': extract_experience_distribution,
                                '證照': extract_experience_distribution
                            }
                            # 遍歷每個圖表容器
                            for container in chart_containers:
                                # 找出標題 DIV
                                title_div = container.find_element(By.CSS_SELECTOR, 'div:first-child')
                                # 找出詳細資訊 DIV
                                details_div = container.find_element(By.CSS_SELECTOR, 'div:last-child')
                                # 獲取標題
                                title = title_div.text
                                # 根據標題提取資料
                                if title in fields:
                                    # 使用對應的提取方法
                                    extraction_method = fields[title]
                                    job_info[title] = extraction_method(details_div)
                        except Exception as e:
                            logging.error(f"獲取應徵詳細資訊時發生錯誤: {e}")
                            job_info = {
                                "學歷": {},
                                "性別": {},
                                "年齡": {},
                                "工作經驗": {},
                                "科系": {},
                                "語言能力": {},
                                "技能": {},
                                "證照": {}
                            } 
                        time.sleep(3)
                        # 更新要存入的資料
                        job_list.append({
                            "職缺名稱":job_name, "職缺網址":job_url, "公司名稱":company, "更新日期":update_date, "積極徵才":actively_hiring, 
                            "應徵人數":applicants, "工作內容":job_description, "職務類別":job_category, "工作待遇":salary, "工作性質":job_type, 
                            "上班地點":location, "管理責任":management, "出差外派":business_trip, "上班時段":work_time, "休假制度":vacation, 
                            "可上班日":start_work, "需求人數":headcount, "工作經歷":work_exp, "學歷要求":education, "科系要求":major, 
                            "語文條件":language, "擅長工具":tools, "工作技能":skills, "具備證照":certificates, "其他條件":other_requirements,
                            "法定福利":legal_benefits_str, "其他福利":other_benefits_str, "原始福利":raw_benefits, "聯絡資訊":contact_info_str, "應徵分析":job_info                        
                        })
                        com_list.append([company_url])
                        logging.info("已添加進陣列")
                    except Exception as e:
                        logging.error(f"處理詳細頁面資訊時發生錯誤: {e}")
                        job_list.append([
                            job_name, job_url, company, update_date, actively_hiring, 
                            applicants, "", "", "", "",
                            "", "", "", "", "",
                            "", "", "", "", "", 
                            "", "", "", "", "", 
                            "", "", "", "", "", # 新增欄位的空值
                        ])
                        com_list.append([""])
                        if sum(1 for field in job_list[-1] if field == "") > 6:
                            crawler_error += 1
                    # 關閉詳細頁面，切回列表頁
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                except Exception as e:
                    logging.error(f"處理 {company}, {job_name}職缺時發生錯誤, {job_url}: {e}")
                    crawler_error += 1
                    continue
            # 滾動到下一頁
            old_scrolls = len(current_jobs)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            # 檢查是否有新職缺載入
            new_jobs = driver.find_elements(By.CSS_SELECTOR, target_selector)
            if len(new_jobs) == len(current_jobs):
                logging.info("沒有新的職缺載入，可能已到底部")
                break
            scrolls += 1
            
        except Exception as e:
            logging.error(f"處理第{scrolls}頁職缺時發生錯誤: {e}")
            break

# 使用方式
crawl_jobs(keyword_list)

crawler_error = 0

save_to_json(job_list, directory='/opt/dagster/data/job_list')
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"com_url_{timestamp}.json"
save_to_json(com_list, filename=filename, directory='/opt/dagster/data/com_url')

driver.quit()
logging.info("職缺爬蟲程式執行完畢")