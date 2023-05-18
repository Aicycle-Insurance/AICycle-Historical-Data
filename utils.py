import csv
import json
import logging
import os
import random
import re
import time
from datetime import date, datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup


PROXY_LIST = [
    '54.169.59.221:8888',
    '13.212.246.164:8888',
    '54.179.91.224:8888',
    '18.142.139.250:8888',
    '54.255.205.203:8888',
    '18.141.189.235:8888',
    '13.229.97.173:8888',
    '3.0.99.75:8888',
    '13.250.39.147:8888',
    '54.179.119.184:8888'
]


def get_proxy():
    ip = random.choice(PROXY_LIST)
    proxy = {
        'https': ip,
        'http': ip,
    }
    return proxy


def get_info_single_page(car_url):
    car_title = None
    id_post = None
    model = None
    company = None
    version = None
    price = None
    year = None
    style = None
    status = None
    origin = None
    transmission = None
    fuel_type = None
    sell_name = None
    sell_phone = None
    public_date = None
    img_link = None
    num_door = None
    num_seat = None
    color_out = None
    color_in = None
    used_km = None
    consumption = None
    drive = None
    fuel_load = None
    description = None

    result = requests.get(f"{car_url}", proxies = get_proxy(), timeout = 30)
    soup = BeautifulSoup(result.text, 'html.parser')
    # Get car_title

    try:
        car_title = soup.find("div", {"id": "car_detail"})
        car_title = car_title.select_one("div.title h1").get_text()
        car_title = car_title.replace('\t', ' ')
    except:
        raise Exception(f"Page {car_url} not found")

    # Get company, model, version
    listText = soup.findAll('span', attrs={'itemprop': 'itemListElement'})
    # if (len(listText)) > 4:
    company_lowercase = listText[2].text.replace('Loading...', '')
    if 'Hãng khác' in listText[2].text:
        company_lowercase = listText[2].text.replace('Loading...', '')
        company = company_lowercase.strip()
        if 'Trước' in car_title:
            index_year_start = car_title.index('Trước')
            index_year_end = car_title.index('-') - 1
            year = car_title[index_year_start:index_year_end]
        else:
            index_year_start = car_title.index('-') - 5
            index_year_end = car_title.index('-') - 1
            year = car_title[index_year_start:index_year_end]
        model = None
        version = car_title[3:(index_year_start -1)]
    else:
        company_lowercase = listText[2].text.replace('Loading...', '')
        company = company_lowercase.strip()
        if listText[3].text.replace('Loading...', '') == 'Khác':
            index_model = car_title.index(company_lowercase) + len(company_lowercase) + 1
            preModel = company_lowercase
            if 'Trước' in car_title:
                index_year_start = car_title.index('Trước')
                index_year_end = car_title.index('-')
                year = car_title[index_year_start: index_year_end]
                preModel = car_title[(index_model): (index_year_end - 1)]
            else:
                preModel = 'Khác'
            model = preModel.strip()
            version = None
        else:
            preModel = listText[3].text.replace('Loading...', '')
            model = preModel
            index_model = car_title.index(preModel)
            if 'Trước' in car_title:
                index_year_start = car_title.index('Trước')
                index_year_end = car_title.index('-') - 1
                year = car_title[index_year_start: index_year_end]
            else:
                year = listText[4].text.replace('Loading...', '')
                index_year_start = car_title.index(year) - 1
            index_version = index_model + 1 + len(preModel)
            version = str(car_title[index_version: index_year_start]).strip()

    if model == '':
        model = None
    if version == '':
        version = None

    # Regex pattern
    pattern = r"\d\s*-\s*((\d*)\s*Tỷ)*\s*((\d*)\s*Triệu)*"

    # Match pattern to input string
    match = re.findall(pattern, car_title)

    # Calculate total value
    price = -1
    if len(match) > 0:
        billions = int(match[-1][1]) if match[-1][1] != '' else 0
        millions = int(match[-1][3]) if match[-1][3] != '' else 0
        price = billions * 1000 + millions

    get_maTin = soup.find('title').text
    id_post = str(get_maTin.split('|')[1].replace(' ', ''))
    raw_public_date = soup.find('div', {'class': 'notes'}).text
    date_patten = "\d{1,2}/\d{2}/\d{4}"
    public_date = re.findall(string=raw_public_date, pattern=date_patten)

    if len(public_date) > 0:
        public_date = public_date[0]
    else:
        public_date = None
    date_obj = datetime.strptime(public_date, '%d/%m/%Y')
    public_date = date_obj.strftime('%Y-%m-%d')

    sell_name = soup.findAll("a", {"class": "cname"})
    if len(sell_name) == 0:
        sell_name = soup.findAll("span", {"class": "cname"})

    if len(sell_name) > 0:
        sell_name = sell_name[0].get_text()
        sell_phone = str(soup.find("span", {"class": "cphone"}).get_text())

    info2_Car = soup.findAll('div', {'class': 'txt_input'})
    origin = info2_Car[0].text
    status = info2_Car[1].text
    style = info2_Car[2].text
    used_km = int(str(info2_Car[3].text).replace(',', '').replace('Km', '').strip())
    color_out = info2_Car[4].text
    color_in = info2_Car[5].text
    num_door = info2_Car[6].text
    num_seat = soup.find('div', {'class': 'inputbox'}).text.strip()
    fuel_type = str(info2_Car[7].text).replace('\t', ' ')
    fuel_load = info2_Car[8].text
    transmission = info2_Car[9].text
    drive = info2_Car[10].text
    consumption = str(info2_Car[11].text).replace('\t', ' ')
    # name_website = self.website_name

    description = soup.find('div', {'class': 'des_txt'}).text

    img_link = ''
    for link in soup.find_all('a', {'class': 'highslide'}):
        img_link = img_link + str(link.get('href') + ', ')

    result_dict = {
        'name': id_post,
        'car_title': car_title,
        'url': f"{car_url}",
        'version': version,
        'model': model,
        'company': company,
        'year': year,
        'price': price,
        'status': status,
        'origin': origin,
        'style': style,
        'transmission': transmission,
        'fuel_type': fuel_type,
        'sell_name': sell_name,
        'sell_phone': sell_phone,
        'public_date': public_date,
        'img_link': img_link,
        'num_door': num_door,
        'num_seat': num_seat,
        'color_in': color_in,
        'color_out': color_out,
        'used_km': used_km,
        'consumption': consumption,
        'drive': drive,
        'fuel_load': fuel_load,
        'description': description
    }
    return result_dict
