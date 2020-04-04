from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup as bs
import time
import json
import threading
from multiprocessing import Process


url = "https://www.marinetraffic.com/en/data/?asset_type=ports&columns=flag,portname,unlocode,vessels_in_port," \
      "vessels_departures,vessels_arrivals,vessels_expected_arrivals,local_time,anchorage,geographical_area_one," \
      "geographical_area_two&flag_in|in|China,Hong%20Kong,Macao|flag_in=CN,HK,MO"



def get_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))


def timer_task():
    try:
        df = open("Marine_1h.json", "a")
        line = get_500_records()
        print(get_time() + ": Done normally")
        df.write(line+'\n')
        df.close()
    except BaseException as e:
        print(e)


def an_hour_task():
    last_hour = 24 # 一定会启动
    while True:
        cur_time = time.localtime(time.time())
        cur_min = cur_time.tm_min
        if cur_min == 0 and cur_time.tm_hour != last_hour:
            print("===============================================================================================")
            print("Detecting current time is integral point: " + get_time() + ": starting crawler...")
            print("===============================================================================================")
            last_hour = cur_time.tm_hour
            p = Process(target=timer_task)
            p.start()
            time.sleep(30*60)
            print("===============================================================================================")
            print("Detecting current time is half integral point: " + get_time() + ": starting crawler...")
            print("===============================================================================================")
            p2 = Process(target=timer_task)
            p2.start()
        time.sleep(5)


def get_row_list(html):
    soup = bs(html, "html.parser")
    wholeList = soup.find_all(role="row")
    rowList = wholeList[-20:]
    portRowList = wholeList[1:21]
    rtvList  = []
    for row, portRow in zip(rowList, portRowList):
        tmpDict = dict()
        for childNode in portRow.contents:
            tmpL = list(childNode.strings)
            if len(tmpL) > 0:
                tmpDict[childNode.attrs['col-id']] = tmpL[0]
        for childNode in row.contents:
            strings = list(childNode.strings)
            tmpDict[childNode.attrs['col-id']] = strings[0] if len(strings)>0 else ""
        rtvList.append(tmpDict)
    return rtvList


def get_500_records():
    opt = webdriver.ChromeOptions()
    opt.add_argument('--headless')
    opt.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=opt)
    driver.get(url)
    WebDriverWait(driver, 120)
    time.sleep(60)
    rtvList = []
    for _ in range(13):
        html = driver.page_source
        rowList = get_row_list(html)
        next_page_botton = driver.find_element_by_css_selector("[title='Next page']")
        next_page_botton.click()
        time.sleep(2)
        rtvList += rowList
    rtvDict = dict()
    rtvDict[get_time()] = rtvList
    driver.quit()
    return json.dumps(rtvDict)


if __name__ == '__main__':
    an_hour_task()
