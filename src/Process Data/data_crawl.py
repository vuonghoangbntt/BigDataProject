#!/usr/bin/python
# -*- coding: utf-8 -*-

import cloudscraper
import bs4
import pandas as pd
import requests
import logging
import argparse

logging.basicConfig(level=logging.INFO)
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--start", dest="start_index", type=int, metavar='<int>', required=True, help="Start index")
parser.add_argument("-e", "--end", dest="end_index", type=int, metavar='<int>', required=True, help="End index")
parser.add_argument("-u", "--url", dest="list_url_dir", type=str,default= './url/url_link.txt',metavar='<str>', help="Input directory")
parser.add_argument("-o", "--output", dest="output_dir", type=str, metavar='<str>', help="Saving directory")
args = parser.parse_args()

BASE_URL = 'https://batdongsan.com.vn/nha-dat-ban'
SCRAPER = cloudscraper.create_scraper(
    browser={
        'browser': 'firefox',
        'platform': 'linux',
        'android': False
    }
)
def get_page_content(url):
   page = SCRAPER.get(url)
   return bs4.BeautifulSoup(page.text.encode('iso-8859-1','ignore'),"html.parser")
def url_crawling(start, end):
  from tqdm.notebook import tqdm
  f = open(f'list_url_{start}_{end}.txt','w')
  f.write(f'CRAWLING FROM PAGE {start} TO PAGE {end}\nBASE_URL: {BASE_URL}\n')
  f.write('-----------------------------------')
  list_tag= []
  loggin.info("START CRAWLING....")
  for idx in tqdm(range(start, end+1)):
    url = f'{BASE_URL}/p{idx}'
    soup = get_page_content(url)
    tags = soup.find_all('a',class_=['js__product-link-for-product-id','re__unreport'])
    if len(tags)<1:
      logging.info(f'Traceback: Page {idx}: {len(tags)} items')
    list_tag.extend([BASE_URL+'/'+tag['href'] for tag in tags])
  f.write('\n'.join(list_tag))
  f.close()
  logging.info('\nDONE!!!!')

### data crawling
fields = ['Tên', 'Mô tả', 'Mức giá', 'Diện tích', 'Loại tin đăng', 'Địa chỉ',
          'Mặt tiền', 'Đường vào', 'Hướng ban công', 'Số tầng', 'Số phòng ngủ',
          'Số toilet', 'Nội thất', 'Pháp lý', 'Tên dự án', 'Chủ đầu tư',
          'Quy mô', 'Ngày đăng', 'Ngày hết hạn', 'Mã tin', 'Phòng ngủ', 'Hướng nhà','Loại tin','url']

def data_crawl_per_record(url):
  soup = get_page_content(url)
  record = {key: None for key in fields}
  ### title
  record['Tên'] = soup.find('title').get_text().strip()
  #print(record['Tên'])
  
  ### Product Info
  for div in soup.find_all('div', class_='re__section-body re__border--std js__section-body'):
    for div1 in div.find_all('div', class_='re__list-standard-1line--md'):
      key = div1.find('span', class_='title').get_text().strip()[:-1]
      value = div1.find('span', class_='value').get_text().strip()
      record[key] = value
      #print(key+': '+value)
  
  ### Description
  record['Mô tả'] = soup.find('div', class_='re__section-body re__detail-content js__section-body js__pr-description js__tracking').get_text().strip()
  ### Price
  div = soup.find('div', class_='re__pr-short-info js__pr-short-info')
  #print('------------------')
  #print('Important Info')
  for li in div.find_all('div', class_='re__pr-short-info-item js__pr-short-info-item'):
    try:
      key = li.find('span', class_='title').get_text().strip()

      value = li.find('span', class_='value').get_text().strip()
      #print(key+': '+value)
      record[key] = value
    except:
      continue
  ### Date time Info
  div = soup.find('div', class_='re__pr-short-info re__pr-config js__pr-config')
  for li in div.find_all('div', class_='re__pr-short-info-item js__pr-config-item'):
    try:
      key = li.find('span', class_='title').get_text().strip()
      value = li.find('span', class_='value').get_text().strip()
      #print(key+': '+value)
      record[key] = value
    except:
      continue
  ### Save url
  record['url'] = url
  return record
### wrap up
def data_crawl(start, end, data_path = '../url/list_url.txt', output_path = './data/'):
  from tqdm import tqdm
  list_url = []
  fail_url = []
  with open(data_path) as f:
    #lines=  f.readlines()
    for line in f:
      list_url.append(line.strip())
  res_dict = {key: [] for key in fields}
  logging.info('START CRAWLING....')
  logging.info(f'RECORD FROM {start} TO {end}')
  logging.info('-------------------------------------')
  for url in tqdm(list_url[start:end]):
    try:
      record = data_crawl_per_record(url)
      if len(record)>len(fields):
        record = data_crawl_per_record(url)
      if len(record)>len(fields):
        print("Fail at record: ",url)
        fail_url.append(url)
        continue
      for key in res_dict:
        res_dict[key].append(record[key])
    except:
      fail_url.append(url)
      logging.info("Fail at record: ",url)
  if len(fail_url)>0:
    with open(output_path+'url_failed_to_load.txt','a+') as f:
      f.write('\n'.join(fail_url))
  pd.DataFrame(res_dict).to_csv(output_path+f'data_{start}_{end}.csv', index=False)
  logging.info('DONE!!')

### Teamwork
### Thinh: 0-50000
### Hiep: 50000-100000
### Duy: 100000-150000
### Hoang 150000-200000
data_crawl(args.start_index, args.end_index, args.list_url_dir, args.output_dir)