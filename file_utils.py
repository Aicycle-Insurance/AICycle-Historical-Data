import os
import json


def create_folder(folder_path: str):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def export_json(post_dict: dict, json_path: str):
    with open(json_path, 'w+', encoding='utf-8') as f:
        json.dump(post_dict, f)
