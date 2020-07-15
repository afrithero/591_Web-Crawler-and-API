import random
import time
import requests
import re
import threading
import os
import configparser
from pymongo import MongoClient
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent

all_records = []
# 1 為台北市的代號, 3 為新北市的代號
region_codes = [1,3]

def generate_soup(href):
    # 建立各物件網頁的soup，以取得網頁資料結構
    user_agent = UserAgent().random
    res = requests.get(href, headers={'user-agent': user_agent})
    soup = bs(res.text, 'lxml')
    return soup

def contact_phone(soup):
    # 解析刊登者之聯絡電話
    if len(soup.select('div.hidtel')[0].text) != 0:
        if len(soup.select('div.hidtel')[0].text) > 11:
            return soup.select('div.hidtel')[0].text.split(' ')[0].replace('-','')
        else:
            return soup.select('div.hidtel')[0].text
    else:
        return soup.select('span.dialPhoneNum')[0].get('data-value').split(' ')[0].replace('-','')

def rent_req(soup):
    # 解析租賃之性別要求
    req_key = [i.text.replace(' ','') for i in soup.select('div.leftBox > ul > li > div.one')]
    req_value = [i.text.strip('：') for i in soup.select('div.leftBox > ul > li > div.two')]
    req_dict = dict(zip(req_key,req_value))
    sex_req = req_dict.setdefault('性別要求', None)
    return sex_req

def house_attr(soup):
    # 解析物件型態及現況
    attr_key = [i.text.split(':')[0].strip() for i in soup.select('div.detailInfo > ul > li')]
    attr_value = [i.text.split(':')[1].strip() for i in soup.select('div.detailInfo > ul > li')]
    attr_dict = dict(zip(attr_key,attr_value))
    house_type = attr_dict.setdefault('型態', None)
    house_condition = attr_dict.setdefault('現況', None)
    return house_type, house_condition

def chrome_driver():
    # Chrome 瀏覽器參數設定
    chrome_options = Options()
    prefs = {"profile.default_content_setting_values.notifications":2}
    chrome_options.add_experimental_option("prefs",prefs)
    chrome_options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"')
    chrome_options.add_argument('--headless')
    curpath = os.path.dirname(os.path.abspath('__file__')) 
    driver_path = os.path.join(curpath,'chromedriver')
    driver = webdriver.Chrome(driver_path,options=chrome_options)
    return driver

def monInsert(data):
    # 將資料插入 MongoDB
    curpath = os.path.dirname(os.path.abspath('__file__')) 
    cfgpath = os.path.join(curpath,'conf.ini')
    conf = configparser.ConfigParser()
    conf.read(cfgpath, encoding="utf-8")
    username = conf.get('mongodb','dbuser')
    password = conf.get('mongodb','dbpasswd')
    monip = conf.get('mongodb','dbip')
    mondb = conf.get('mongodb','dbname')
    moncol= conf.get('mongodb','col')
    monclient = MongoClient(monip,
                username = username,
                password = password,
                authSource = mondb,
                authMechanism = 'SCRAM-SHA-256')
    mondb = monclient[mondb]
    collection = mondb[moncol]
    collection.insert_many(data,ordered=False)

def main(code):
    # 主程式
    global all_records
    house_links = []

    driver = chrome_driver()
    driver.delete_all_cookies()
    wait = WebDriverWait(driver, 10)
    driver.get('https://rent.591.com.tw/?kind=0')
    element_location = "dd[data-id='{}']".format(code)
    time.sleep(5)
    driver.find_element_by_css_selector(element_location).click()
    print('開始爬取物件連結')

    while True:
        while True:
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,"h3")))
                break
            except TimeoutException:
                print('再試定位此頁物件連結')
                continue

        time.sleep(5)
        
        for j in driver.find_elements_by_css_selector('h3 > a'):
            house_links.append(j.get_attribute('href'))
        
        print('目前爬入筆數',len(house_links),'---',threading.current_thread())

        if driver.find_element_by_css_selector('a.pageNext').get_attribute('href') is None:
            driver.quit()
            print(threading.current_thread(),'已爬入所有物件連結')
            break

        else:
            while True:
                try:
                    next_page = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.pageNext")))
                except TimeoutException:
                    print("再試點擊下一頁")
                    continue
                else:
                    next_page.location_once_scrolled_into_view
                    print('準備跳下一頁','---',threading.current_thread())
                    next_page.click()
                    break

    
    print(threading.current_thread(),'開始解析物件資料')

    for i in house_links:

        current_record = {}
        soup = generate_soup(i)

        try:
            current_record['縣市'] = soup.select('#propNav > a')[2].text
            current_record['鄉鎮'] = soup.select('#propNav > a')[3].text
            current_record['出租者'] = re.split('\(|\（',soup.select('div.avatarRight > div')[0].text)[0].strip()
            current_record['出租者身份'] = re.search('屋主|仲介|代理人', soup.select('div.avatarRight > div')[0].text).group()
            current_record['聯絡電話'] = contact_phone(soup)
            house_type, house_condition = house_attr(soup)
            current_record.update({'型態': house_type, '現況': house_condition})
            current_record['性別要求'] = rent_req(soup)
        
        except:
            print('此物件頁面有異常，暫時跳過')
            continue

        all_records.append(current_record)


if __name__ == '__main__':

    thread_list = []
    for region_code in region_codes:
        t = threading.Thread(name='region_{}'.format(region_code), target=main, args=(region_code,))
        t.start()
        print(t.name + '啟動！')
        thread_list.append(t)

    # 封鎖子執行緒
    for thread in thread_list:
        thread.join()
    
    print('總資料筆數',len(all_records))

    monInsert(all_records)

    print('入庫成功')
