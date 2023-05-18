import random
import time
from concurrent.futures.thread import ThreadPoolExecutor

import chromedriver_autoinstaller
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from file_utils import create_folder, export_json
from utils import get_info_single_page
from constants import JSON_FOLDER, ROOT_FOLDER, COMPLETED_FILE, FAILED_FILE, ID_FILE, COMPLETED_ID_FILE

create_folder(ROOT_FOLDER)
create_folder(JSON_FOLDER)

df = []

proxies = [
    {'ip': '54.169.59.221', 'port': '8888'},
    {'ip': '18.142.139.250', 'port': '8888'},
    {'ip': '18.141.189.235', 'port': '8888'},
    {'ip': '3.0.99.75', 'port': '8888'},
    {'ip': '13.212.246.164', 'port': '8888'},
    {'ip': '54.179.91.224', 'port': '8888'},
    {'ip': '54.255.205.203', 'port': '8888'},
    {'ip': '13.229.97.173', 'port': '8888'},
    {'ip': '13.250.39.147', 'port': '8888'},
    {'ip': '54.179.119.184', 'port': '8888'}
]


def create_driver_with_proxy():
    # Chọn ngẫu nhiên một proxy từ danh sách
    random_proxy = random.choice(proxies)
    proxy_ip = random_proxy['ip']
    proxy_port = random_proxy['port']
    # Tạo tùy chọn của trình duyệt Chrome với proxy ngẫu nhiên đã chọn
    chrome_options = Options()
    chrome_options.headless = False
    chrome_options.add_argument("--disable-features=Permissions-Policy")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}
    chrome_options.add_argument(f'--proxy-server={proxy_ip}:{proxy_port}')

    # Khởi tạo trình điều khiển Chrome với tùy chọn đã cấu hình
    driver = webdriver.Chrome(chromedriver_autoinstaller.install(), options=chrome_options)
    return driver


dataframe = pd.DataFrame()
final_df = []
completed_links = []
completed_ids = []


def run_selenium_task(i, start_id, end_id):
    # Khởi tạo driver và thực hiện các thao tác cần thiết với ID trong phạm vi
    driver = create_driver_with_proxy()
    for id in range(start_id, end_id + 1):
        # Kiểm tra ID đã hoàn thành hay chưa
        if id not in completed_ids:
            driver.get(f'https://duckduckgo.com/?q={id}+inurl%3Abonbanh.com')
            try:
                driver.find_element(By.CLASS_NAME, 'wLL07_0Xnd1QZpzpfR4W')
            except:
                driver.quit()
                driver = create_driver_with_proxy()
                driver.get(f'https://duckduckgo.com/?q={id}+inurl%3Abonbanh.com')

            tags = driver.find_elements(By.CLASS_NAME, 'wLL07_0Xnd1QZpzpfR4W')

            set_links = set()
            for tag in tags[:5]:
                # print(tag)
                link = tag.find_element(By.CLASS_NAME, 'Rn_JXVtoPVAFyGkcaXyK')
                link = (link.get_attribute("href"))
                if link.startswith("https://bonbanh.com/xe-"):
                    set_links.add(link)

            for link in set_links:
                try:
                    with open(ID_FILE, "a+") as f:
                        f.write(link + "\n")
                    post_dict = get_info_single_page(link)
                    id = link.split('-')[-1]
                    export_json(post_dict, json_path=f'{JSON_FOLDER}/{id}.json')
                    final_df.append(post_dict)
                    completed_ids.append(id)
                    completed_links.append(link)
                    with open(COMPLETED_FILE, "a+") as file:
                        file.write(link + "\n")
                    with open(COMPLETED_ID_FILE, "a+") as file:
                        file.write(id + "\n")
                except Exception as ex:
                    print(link + 'failed')
                    with open(FAILED_FILE, "a+") as file:
                        file.write(link + "\n")


try:
    with open(COMPLETED_FILE, "r") as file:
        completed_links = [line.strip().replace('\n', '') for line in file.readlines()]
        # completed_ids = [link.split('-')[-1] for link in completed_links]
    with open(COMPLETED_ID_FILE, 'r') as file:
        completed_ids = [line.strip().replace('\n', '') for line in file.readlines()]
except FileNotFoundError:
    pass


def run_main(start_id: int = 4600000, end_id: int = 4600100, num_threads: int = 3):
    # Số lượng ID trong mỗi thread
    ids_per_thread = (end_id - start_id + 1) // num_threads

    # Khởi tạo ThreadPoolExecutor với số lượng thread tương ứng
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Chia ID thành các phạm vi tương ứng cho mỗi thread và chạy công việc
        for trial_id in range(num_threads):
            # Tính toán start_id và end_id cho mỗi thread
            start_id_thread = start_id + trial_id * ids_per_thread
            end_id_thread = start_id_thread + ids_per_thread - 1
            # Chạy công việc trên mỗi thread
            executor.submit(run_selenium_task, trial_id, start_id_thread, end_id_thread)

    final_dataframe = pd.DataFrame(final_df)
    final_dataframe.to_csv(f"{ROOT_FOLDER}/{start_id}_{end_id}.csv")


run_main()
