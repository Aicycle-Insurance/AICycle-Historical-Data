

import pandas as pd
from utils import get_info_single_page
from tqdm import tqdm 
import json
import glob
from file_utils import create_folder, export_json
from joblib import Parallel, delayed
list_file = glob.glob("./data/json/*.json")
file_objects = []
for file in list_file:
    with open(file, 'r') as f:
        file_object = json.load(f)
        file_objects.append(file_object)

df = pd.DataFrame(file_objects)

with open("data/id_all.txt", 'r') as f:
    crawled_ids = []
    linked_ids = []
    all_lines = f.readlines()
    for x in all_lines:
        crawled_ids.append(int(x.split("-")[-1]))
        linked_ids.append(str(x))
    
crawled_dict = dict(zip(crawled_ids, linked_ids)) 

print(len(df.name.tolist()))
not_crawled_ids = set(crawled_ids) - set(df.name.tolist())
print("ALL CRAWLED IDS:", len(set(crawled_ids)))
print("NOT CRAWLED IDS:", len(not_crawled_ids))
df.to_csv("raw1.csv", index=False)

print("Start step 2 to crawl data:")
create_folder("data/newjson")
def CRAWLER(link):
    key, value = link
    if key not in set(df.name.tolist()):
        post_dict = get_info_single_page(value)
        export_json(post_dict, json_path=f"data/newjson/{key}.json")

Parallel(n_jobs=20, backend="threading")(delayed(CRAWLER)(link) for link in tqdm(crawled_dict.items()))
