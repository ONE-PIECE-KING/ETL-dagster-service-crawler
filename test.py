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
    log_filename = os.path.join(log_dir, f'job_crawler_{datetime.now().strftime("%Y%m%d_%H%M%S_%f")}.log')
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
    return logger

def is_similar_rgb(rgb_str, target_rgb):
    rgb_values = [int(x) for x in rgb_str.replace("rgb(", "").replace(")", "").split(",")]
    tolerance = 5
    return all(abs(a - b) <= tolerance for a, b in zip(rgb_values, target_rgb))

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

def save_to_json(job_data_list, filename=None, mode='w', directory='default_directory'):
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"job_details_{timestamp}.json"
    if not filename.endswith('.json'):
        filename += '.json'
    if not os.path.exists(directory):
        os.makedirs(directory)
    file_path = os.path.join(directory, filename)
    try:
        if mode == 'a' and os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            job_data_list = existing_data + job_data_list
        with open(file_path, mode, encoding='utf-8') as f:
            json.dump(job_data_list, f, ensure_ascii=False, indent=4)
        logging.info(f"資料已成功儲存至 {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"儲存 JSON 檔案時發生錯誤: {e}")
        return None

def run_crawler(keyword_list, max_errors=3, output_directory='/opt/dagster/data/job_list'):
    """
    執行爬蟲，依關鍵字抓取工作資料，並儲存為 JSON 檔案。
    保留原有所有欄位擷取邏輯，並回傳 JSON 檔案路徑與 job_list 資料。
    """
    logger = setup_logging()
    logger.info("啟動職缺爬蟲")
    chrome_options = Options()
    ua = UserAgent()
    chrome_options.add_argument(f'user-agent={ua.random}')
    chrome_options.add_argument('--start-maximized')
    chrome_options.add_argument('--remote-debugging-port=9222')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--disable-dev-shm-usage')
    if os.path.exists("/usr/bin/chromium"):
        chrome_options.binary_location = "/usr/bin/chromium"
        service = Service("/usr/bin/chromedriver")
    else:
        service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    target_selector = 'div.job-summary'
    max_scrolls = 1  # 測試時僅執行一次頁面
    job_list = []
    com_list = []
    for keyword in keyword_list:
        try:
            url = f"https://www.104.com.tw/jobs/search/?keyword={keyword}"
            driver.get(url)
            time.sleep(5)
            process_jobs(driver, target_selector, logger, job_list, com_list, max_scrolls)
        except Exception as e:
            logger.error(f"爬蟲 {keyword} 發生錯誤: {e}")
    driver.quit()
    logger.info("職缺爬蟲程式執行完畢")
    # 儲存 job_list 為 JSON 檔案
    file_path = save_to_json(job_list, directory=output_directory)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"com_url_{timestamp}.json"
    save_to_json(com_list, filename=filename, directory='/opt/dagster/data/com_url')
    return file_path, job_list

def process_jobs(driver, target_selector, logger, job_list, com_list, max_scrolls):
    scrolls = 0
    crawler_error = 0
    while scrolls < max_scrolls:
        try:
            logger.info(f"正在處理第 {scrolls + 1} 頁...")
            current_jobs = driver.find_elements(By.CSS_SELECTOR, target_selector)
            logger.info(f"當前頁面職缺數量: {len(current_jobs)}")
            # 這邊以第一筆作示範，若有需要可擴充處理多筆
            for job in current_jobs[:1]:
                try:
                    title_element = job.find_element(By.CSS_SELECTOR, 'h2 a.info-job__text')
                    job_url = title_element.get_attribute('href')
                    job_name = title_element.get_attribute('title')
                    company_element = job.find_element(By.CSS_SELECTOR, 'a[data-gtm-joblist="職缺-公司名稱"]')
                    company = company_element.text.strip()
                    company_url = company_element.get_attribute('href')
                    driver.execute_script(f"window.open('{job_url}', '_blank')")
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(3)
                    try:
                        logger.info(f"職缺名稱: {job_name}")
                        logger.info(f"職缺網址: {job_url}")
                        logger.info(f"公司名稱: {company}")
                        update_date_element = driver.find_element(By.CSS_SELECTOR, 'span.text-gray-darker[title*="更新"]')
                        update_date = update_date_element.get_attribute('title')
                        update_date = update_date.replace("更新", "").strip()
                        logger.info(f"更新日期: {update_date}")
                        try:
                            actively_hiring = driver.find_element(By.CSS_SELECTOR, 'div.actively-hiring-tag').text.strip()
                            actively_hiring = "是" if actively_hiring == "積極徵才中" else "否"
                        except:
                            actively_hiring = "否"
                        try:
                            applicants = driver.find_element(By.CSS_SELECTOR, 'a.d-flex.align-items-center.font-weight-bold').text.strip()
                            applicants = applicants.replace("應徵人數", "").replace("人", "").strip()
                        except Exception as e:
                            applicants = "N/A"
                            logger.info(f"獲取應徵人數時發生錯誤: {e}")
                        job_description = driver.find_element(By.CSS_SELECTOR, 'p.job-description__content').text.strip()
                        job_categories = driver.find_elements(By.CSS_SELECTOR, 'div.category-item u')
                        job_category = '、'.join([cat.text for cat in job_categories])
                        salary = driver.find_element(By.CSS_SELECTOR, 'p.text-primary.font-weight-bold').text.strip()
                        job_type = driver.find_element(By.CSS_SELECTOR, 'div.list-row:nth-child(4) div.list-row__data').text.strip()
                        location = driver.find_element(By.CSS_SELECTOR, 'div.job-address span').text.strip()
                        management_elements = driver.find_elements(By.CSS_SELECTOR, 'div.list-row')
                        management = ""
                        for element in management_elements:
                            try:
                                title_text = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title_text == "管理責任":
                                    management = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取管理責任時發生錯誤: {e}")
                                continue
                        business_trip = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "出差外派":
                                    business_trip = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取出差外派時發生錯誤: {e}")
                                continue
                        work_time = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "上班時段":
                                    work_time = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取上班時段時發生錯誤: {e}")
                                continue
                        vacation = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "休假制度":
                                    vacation = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取休假制度時發生錯誤: {e}")
                                continue
                        start_work = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "可上班日":
                                    start_work = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取可上班日時發生錯誤: {e}")
                                continue
                        headcount = ""
                        for element in management_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "需求人數":
                                    headcount = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取需求人數時發生錯誤: {e}")
                                continue
                        work_exp = ""
                        work_exp_elements = driver.find_elements(By.CSS_SELECTOR, 'div.list-row')
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "工作經歷":
                                    work_exp = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取工作經歷時發生錯誤: {e}")
                                continue
                        education = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "學歷要求":
                                    education = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取學歷要求時發生錯誤: {e}")
                                continue
                        major = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "科系要求":
                                    major = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取科系要求時發生錯誤: {e}")
                                continue
                        language = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "語文條件":
                                    language = element.find_element(By.CSS_SELECTOR, 'div.list-row__data').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取語文條件時發生錯誤: {e}")
                                continue
                        tools = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "擅長工具":
                                    tools_elements = element.find_elements(By.CSS_SELECTOR, 'div.list-row__data u')
                                    tools = '、'.join([tool.text for tool in tools_elements])
                                    break
                            except Exception as e:
                                logger.error(f"獲取擅長工具時發生錯誤: {e}")
                                continue
                        skills = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "工作技能":
                                    skills_elements = element.find_elements(By.CSS_SELECTOR, 'div.list-row__data u')
                                    skills = '、'.join([skill.text for skill in skills_elements])
                                    break
                            except Exception as e:
                                logger.error(f"獲取工作技能時發生錯誤: {e}")
                                continue
                        certificates = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "具備證照":
                                    cert_elements = element.find_elements(By.CSS_SELECTOR, 'div.list-row__data u')
                                    certificates = '、'.join([cert.text for cert in cert_elements])
                                    break
                            except Exception as e:
                                logger.error(f"獲取具備證照時發生錯誤: {e}")
                                continue
                        other_requirements = ""
                        for element in work_exp_elements:
                            try:
                                title = element.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                                if title == "其他條件":
                                    other_requirements = element.find_element(By.CSS_SELECTOR, 'div.list-row__data p.r3').text.strip()
                                    break
                            except Exception as e:
                                logger.error(f"獲取其他條件時發生錯誤: {e}")
                                continue
                        try:
                            legal_benefits = []
                            legal_elements = driver.find_elements(By.CSS_SELECTOR, 'div.benefits-labels:nth-child(3) span.tag--text a')
                            legal_benefits = [item.text.strip() for item in legal_elements]
                            legal_benefits_str = '、'.join(legal_benefits)
                            
                            other_elements = driver.find_elements(By.CSS_SELECTOR, 'div.benefits-labels:nth-child(5) span.tag--text a')
                            other_benefits = [item.text.strip() for item in other_elements]
                            other_benefits_str = '、'.join(other_benefits)
                            
                            raw_benefits = ""
                            benefits_description = driver.find_element(By.CSS_SELECTOR, 'div.benefits-description p.r3').text.strip()
                            raw_benefits = benefits_description
                        except Exception as e:
                            logger.error(f"獲取福利制度時發生錯誤: {e}")
                            legal_benefits_str = ""
                            other_benefits_str = ""
                            raw_benefits = ""
                        
                        try:
                            contact_info = []
                            contact_elements = driver.find_elements(By.CSS_SELECTOR, 'div.job-contact-table div.job-contact-table__data')
                            contact_info = [element.text.strip() for element in contact_elements]
                            contact_info_str = '\n'.join(contact_info)
                        except Exception as e:
                            logger.error(f"獲取聯絡方式時發生錯誤: {e}")
                            contact_info_str = ""
    
                        try:
                            apply_code = job_url.split('/')[-1].split('?')[0]
                            apply_analysis_url = f"https://www.104.com.tw/jobs/apply/analysis/{apply_code}"
                            driver.execute_script(f"window.open('{apply_analysis_url}', '_blank')")
                            driver.switch_to.window(driver.window_handles[-1])
                            job_info = {}
                            try:
                                apply_education = {}
                                education_elements = driver.find_elements(By.CSS_SELECTOR, "div.legend__text")
                                education_values = driver.find_elements(By.CSS_SELECTOR, "div.legend__value") 
                                for i in range(len(education_elements)):
                                    apply_education[education_elements[i].text] = education_values[i].text
                                job_info["學歷"] = apply_education
                            except Exception as e:
                                logger.error(f"獲取聯絡方式時發生錯誤: {e}")
                            logger.info(job_info)
                            gender = {}
                            gender_elements = driver.find_elements(By.CSS_SELECTOR, ".stack-bar__text__block")
                            for element in gender_elements[:2]:
                                style = element.get_attribute("style")
                                rgb_value = style[style.find("rgb"):style.find(")") + 1]
                                gender_text = element.find_element(By.CSS_SELECTOR, "div").text
                                male_rgb = [78, 145, 255]
                                female_rgb = [255, 144, 199]
                                if is_similar_rgb(rgb_value, male_rgb):
                                    gender["男性"] = gender_text
                                elif is_similar_rgb(rgb_value, female_rgb):
                                    gender["女性"] = gender_text
                            job_info["性別"] = gender
                            logger.info(job_info)
                            language_container = driver.find_elements(By.CSS_SELECTOR, "div.chart-container__body")[5]
                            logger.info(language_container)
                            language_skills = {}
                            
                            language_items = language_container.find_elements(By.XPATH, ".//div[contains(@class, 'mb-4')]")
                            logger.info(language_items)
                            for language_item in language_items:
                                language_name = language_item.find_element(By.XPATH, ".//span[contains(@class, 'text-truncate')]").text
                                logger.info(language_name)
                                skill_bars = language_item.find_elements(By.XPATH, ".//div[contains(@class, 'stack-bar__text__block')]")
                                logger.info(skill_bars)
                                language_description = []
                                legend_map = {
                                    "rgb(255, 231, 217)": "不會",
                                    "rgb(255, 213, 189)": "略懂",
                                    "rgb(255, 195, 161)": "中等",
                                    "rgb(204, 156, 129)": "精通"
                                }
                                for bar in skill_bars:
                                    try:
                                        percentage = bar.text
                                        background_color = bar.get_attribute('style').split('background:')[1].split(';')[0].strip()
                                        skill_level = legend_map.get(background_color, "未知")
                                        language_description.append(f"{skill_level}{percentage}")
                                    except Exception as level_error:
                                        print(f"提取{language_name}技能等級時出錯: {level_error}")
                                language_skills[language_name] = ','.join(language_description)
                            job_info['語言能力'] = language_skills
                            chart_containers = driver.find_elements(By.CSS_SELECTOR, 'div.chart-container.d-flex.flex-column.bg-white.overflow-hidden.horizontal-bar-chart')
                            fields = {
                                '年齡': extract_age_distribution,
                                '工作經驗': extract_experience_distribution,
                                '科系': extract_experience_distribution,
                                '技能': extract_age_distribution,
                                '證照': extract_experience_distribution
                            }
                            for container in chart_containers:
                                title_div = container.find_element(By.CSS_SELECTOR, 'div:first-child')
                                details_div = container.find_element(By.CSS_SELECTOR, 'div:last-child')
                                title = title_div.text
                                if title in fields:
                                    extraction_method = fields[title]
                                    job_info[title] = extraction_method(details_div)
                        except Exception as e:
                            logger.error(f"獲取應徵詳細資訊時發生錯誤: {e}")
                            job_info = {
                                "學歷": {},
                                "性別": {},
                                "年齡": {},
                                "工作經歷": {},
                                "科系": {},
                                "語言能力": {},
                                "技能": {},
                                "證照": {}
                            } 
                        time.sleep(3)
                        job_list.append({
                            "職缺名稱": job_name, 
                            "職缺網址": job_url, 
                            "公司名稱": company, 
                            "更新日期": update_date, 
                            "積極徵才": actively_hiring, 
                            "應徵人數": applicants, 
                            "工作內容": job_description, 
                            "職務類別": job_category, 
                            "工作待遇": salary, 
                            "工作性質": job_type, 
                            "上班地點": location, 
                            "管理責任": management, 
                            "出差外派": business_trip, 
                            "上班時段": work_time, 
                            "休假制度": vacation, 
                            "可上班日": start_work, 
                            "需求人數": headcount, 
                            "工作經歷": work_exp, 
                            "學歷要求": education, 
                            "科系要求": major, 
                            "語文條件": language, 
                            "擅長工具": tools, 
                            "工作技能": skills, 
                            "具備證照": certificates, 
                            "其他條件": other_requirements,
                            "法定福利": legal_benefits_str, 
                            "其他福利": other_benefits_str, 
                            "原始福利": raw_benefits, 
                            "聯絡資訊": contact_info_str, 
                            "應徵分析": job_info                        
                        })
                        com_list.append([company_url])
                        logger.info("已添加進陣列")
                    except Exception as e:
                        logger.error(f"處理詳細頁面資訊時發生錯誤: {e}")
                        job_list.append({
                            "職缺名稱": job_name, 
                            "職缺網址": job_url, 
                            "公司名稱": company, 
                            "更新日期": update_date, 
                            "積極徵才": actively_hiring, 
                            "應徵人數": applicants, 
                            "工作內容": "", 
                            "職務類別": "", 
                            "工作待遇": "", 
                            "工作性質": "",
                            "上班地點": "", 
                            "管理責任": "", 
                            "出差外派": "", 
                            "上班時段": "", 
                            "休假制度": "", 
                            "可上班日": "", 
                            "需求人數": "", 
                            "工作經歷": "", 
                            "學歷要求": "", 
                            "科系要求": "", 
                            "語文條件": "", 
                            "擅長工具": "", 
                            "工作技能": "", 
                            "具備證照": "", 
                            "其他條件": "", 
                            "法定福利": "", 
                            "其他福利": "", 
                            "原始福利": "", 
                            "聯絡資訊": "", 
                            "應徵分析": {}
                        })
                        com_list.append([""])
                        if sum(1 for field in job_list[-1].values() if field == "") > 6:
                            crawler_error += 1
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                except Exception as e:
                    logger.error(f"處理 {company}, {job_name}職缺時發生錯誤, {job_url}: {e}")
                    crawler_error += 1
                    continue
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_jobs = driver.find_elements(By.CSS_SELECTOR, target_selector)
            if len(new_jobs) == len(current_jobs):
                logger.info("沒有新的職缺載入，但仍強制執行")
            scrolls += 1
            
        except Exception as e:
            logger.error(f"處理第{scrolls}頁職缺時發生錯誤: {e}")
            break
