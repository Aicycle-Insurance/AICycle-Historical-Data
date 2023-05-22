import logging
import random
import time
from concurrent.futures.thread import ThreadPoolExecutor

import chromedriver_autoinstaller
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import requests
from bs4 import BeautifulSoup

import tqdm
from file_utils import create_folder, export_json
from utils import get_info_single_page
from constants import (
    JSON_FOLDER,
    ROOT_FOLDER,
    COMPLETED_FILE,
    FAILED_FILE,
    ID_FILE,
    COMPLETED_ID_FILE,
)
from proxy import LIST_SERVER

logger = logging.getLogger(__name__)



def get_proxy():
    ip = random.choice(LIST_SERVER)
    logger.info(f"Request to {ip}")
    proxy = {
        'https': ip,
        'http': ip,
    }
    return proxy

dataframe = pd.DataFrame()
final_df = []
completed_links = []
completed_ids = []



def run_selenium_task(i, start_id, end_id):
    while True:
        try:
            for id in tqdm.tqdm(range(start_id, end_id + 1)):
                if id not in completed_ids:
                    session = requests.Session()
                    result = requests.get(f'https://search.yahoo.com/search?p={id}+bonbanh', timeout=5)
                    # result = requests.get(f'https://search.yahoo.com/search?p={id}+bonbanh',proxies = get_proxy(), timeout=5)
                    time.sleep(2)
                    soup = BeautifulSoup(result.content.decode('utf-8'), 'html.parser')
                    proxy_used = session.proxies
                    if result.status_code != 200:
                        proxy_used = session.proxies
                        print('Error')
                        run_selenium_task(i, start_id, end_id)
                    else:
                        try:
                            get_tag = soup.find_all('div',{'class':'compTitle options-toggle'})
                        except Exception as e:
                            with open('failed.txt', "a+") as file:
                                file.write(e + "\n")
                        get_tag = soup.find_all('div',{'class':'compTitle options-toggle'})
                        set_links = set()
                        for tag in get_tag:
                            # print(tag)
                            get_link = tag.find('a')
                            
                            link = get_link['href']
                            if link.startswith("https://bonbanh.com/xe-"):
                                print(link)
                                set_links.add(link)

                        for link in set_links:
                            try:
                                with open(ID_FILE, "a+") as f:
                                    f.write(link + "\n")
                                post_dict = get_info_single_page(link)

                                idx = link.split("-")[-1]
                                export_json(post_dict, json_path=f"{JSON_FOLDER}/{idx}.json")
                                final_df.append(post_dict)
                                completed_ids.append(idx)
                                completed_links.append(link)

                                with open(COMPLETED_FILE, "a+") as file:
                                    file.write(link + "\n")

                            except Exception as ex:
                                print(link + "failed")
                                with open(FAILED_FILE, "a+") as file:
                                    file.write(link + "\n")
                        with open(COMPLETED_ID_FILE, "a+") as file:
                            file.write(str(id)+ "\n")
            break  
        except Exception as e:
            print(f"Error: {str(e)}")


try:
    with open(COMPLETED_FILE, "r") as file:
        completed_links = [line.strip().replace("\n", "") for line in file.readlines()]
        # completed_ids = [link.split('-')[-1] for link in completed_links]
    with open(COMPLETED_ID_FILE, "r") as file:
        completed_ids = [int(line.strip().replace('\n', '')) for line in file.readlines() if line.strip().replace('\n', '').isdigit()]
        completed_ids = list(set(completed_ids))
except FileNotFoundError:
    pass

def run_main(start_id: int = 4600000, end_id: int = 4600100, num_threads: int = 2):
   
    ids_per_thread = (end_id - start_id + 1) // num_threads

    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        
        for trial_id in range(num_threads):
            
            start_id_thread = start_id + trial_id * ids_per_thread
            end_id_thread = start_id_thread + ids_per_thread - 1
            
            executor.submit(run_selenium_task, trial_id, start_id_thread, end_id_thread)

    final_dataframe = pd.DataFrame(final_df)
    final_dataframe.to_csv(f"{ROOT_FOLDER}/{start_id}_{end_id}.csv")


import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--start", type=int, default=4_000_000, required=True)
parser.add_argument("--end", type=int, default=4_100_000, required=True)
parser.add_argument("--thread", type=int, default=2, required=True)


if __name__ == "__main__":
    args = parser.parse_args()
    run_main(start_id=args.start, end_id=args.end, num_threads=args.thread)

# python yahoo.py --start 3500000 --end 5000000 --thread 1000