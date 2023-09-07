#-*- coding:utf-8 -*-

import os
import re
import sys
import time
import numpy as np
import pandas as pd
import re
import datetime
import pickle
import csv
import random
from urllib import parse



import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
import subprocess
import shutil
import socket

import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import IntegrityError

import urllib

# Exception Error Handling
import warnings
warnings.filterwarnings("ignore")



class DB:
    def __init__(self):
        pass

    def db_connection(self):
        host = "localhost"
        port = 3306
        database = "Report"
        username = "root"
        password = "1234qwer"

    
        db_connections = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
        engine = create_engine(db_connections, pool_recycle=600)
        conn = engine.connect()

        return engine, conn





class StartDriver:

    def __init__(self):
        pass
       
  
    def StartDriver(self):


        option = Options()
        option.add_argument('--incognito')
        option.add_argument('--no-sandbox')
        option.add_argument('--disable-setuid-sandbox')
        option.add_argument('--disable-dev-shm-usage')
        option.add_argument("disable-gpu")
        option.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36")
  
        driver = webdriver.Chrome('./chromedriver.exe', options=option)
        driver.implicitly_wait(3)
        time.sleep(5)


        return driver


class FindElement:

    def __init__(self):
        pass


    def FindeElementByXPath(self, driver, xpath):

        try :
            val = driver.find_elements(By.XPATH, xpath)
        except:
            val = []

        return val



    def FindeElementByXPath_one(self, driver, xpath):
    
        try:
            val = driver.find_element(By.XPATH, xpath)
        except:
            val=""
            return val

        return val



    def FindeElementByCSS(self, driver, css_selector):
    
    
        val = driver.find_element(By.CSS_SELECTOR, css_selector).text

        return val




def Random(randomA, randomB):

    x = random.randint(randomA, randomA)
    return x


def Encoder(keyword):

    res = parse.quote(keyword)

    return res


def Getfilesize(url):

    res = requests.get(url)
    filesize = 0
    
    time.sleep(3)
    if res.status_code == 200:
        try:
            filesize = int(res.headers['Content-Length'])
        except:
            filesize = 0

    return filesize


def Scrolldown(driver):

	try:
		for i in range(Random(1,5)):
			driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
			time.sleep(1.5)
	except:
		pass





def Crawling(driver, engine, conn, keyword, host, page_offset, history_index):

		
	offset = page_offset
	status = 0
	condition = Encoder(f'site:{host} AND (filetype:pdf OR filetype:hwp OR filetype:zip)')
	e = ''
	try:
		while True:
			baseurl = f'https://www.google.com/search?q={keyword}+{condition}&start={offset}'
			driver.get(baseurl)
			time.sleep(5)
			driver.maximize_window()
			driver.implicitly_wait(10)

			searchlist = FindElement.FindeElementByXPath(FindElement, driver, '//div[@class="yuRUbf"]/div/a')
			

			if len(searchlist) != 0:
				for search in searchlist:
					try:
						url= search.get_attribute('href')
						try:
							query = f'INSERT INTO Report.pdfurl(`url`) VALUES ("{url}");'
							conn.execute(text(query))
							conn.commit()

						except IntegrityError:
							pass
					
					except AttributeError :
						pass

			

			else:
				captcha = FindElement.FindeElementByXPath_one(FindElement, driver,'//iframe[@title="reCAPTCHA"]')
				if captcha != '':
					status = -1
				
				else:
					status = 1
					
				break

			time.sleep(Random(5,10))
			Scrolldown(driver)

			offset+=10
			dump_sql = f"UPDATE Report.history SET offset = {offset} WHERE idx={history_index};"
			conn.execute(text(dump_sql))
			conn.commit()

			time.sleep(Random(30,45))
		
			Scrolldown(driver)
			


	except Exception as e:
		print(e)
		status = -1
			

	
	return status, offset


if __name__ == "__main__":
    

	DB = DB()
	engine, conn = DB.db_connection()

	unlocking = "UNLOCK TABLES;"
	conn.execute(text(unlocking))
	conn.commit()

	try:
	
		while True:
			try:
				#History Table
				"""
				status=0, 수집 대기중
				status=-1, 수집 실패(캡챠 등의 이슈로 멈춘것)
				status=1, 수집 완료
				status=2, 수집 진행중
				"""
			
				#Concurrency Control
				locking = "LOCK TABLE Report.history WRITE;"
				conn.execute(text(locking))
				conn.commit()


				sql = "SELECT idx, kidx, sidx, offset FROM Report.history WHERE status=0 LIMIT 1;"
				result = conn.execute(text(sql)).fetchall()
				print(result)
			
				idx, kidx, sidx, offset = result[0][0],result[0][1],result[0][2],result[0][3]
				
				update_sql = f"UPDATE Report.history SET status=2 WHERE idx={idx};"
				conn.execute(text(update_sql))
				conn.commit()

				unlocking = "UNLOCK TABLES;"
				conn.execute(text(unlocking))
				conn.commit()


				sql = f"SELECT keyword, host FROM Report.history_vw WHERE idx={idx};"
				res = conn.execute(text(sql))
				res = res.fetchall()
				keyword, host = res[0][0], res[0][1]
			

				driver = StartDriver().StartDriver()
				status, new_offset = Crawling(driver, engine, conn, Encoder(keyword), host, offset, idx)
				

				dump_sql = f"UPDATE Report.history SET offset = {new_offset}, status = {status} WHERE kidx={kidx} and sidx={sidx};"
				conn.execute(text(dump_sql))
				conn.commit()


				driver.quit()


				if sidx==116:
			
					dump_sql = f"UPDATE Report.keyword SET status = 1 WHERE idx={kidx};"
					conn.execute(text(dump_sql))
					conn.commit()


			except Exception as e:
				print(e)

			except OperationalError as e:
				print(e)
				print("## continue")
				continue

	except:
		pass
	
	finally:
		unlocking = "UNLOCK TABLE;"
		conn.execute(text(unlocking))
		conn.commit()
		
		conn.close()

    
