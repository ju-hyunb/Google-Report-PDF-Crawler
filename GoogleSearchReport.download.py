#-*- coding:utf-8 -*-

import os
import os
import sys

import pandas as pd
import re
import datetime
import random
import time
from tqdm import tqdm

import socket
from urllib import parse
import urllib.request
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import requests
import wget

import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError

import warnings
warnings.filterwarnings("ignore")



def db_connection():


    host = "localhost"
    port = 3306
    database = "Report"
    username = "root"
    password = "1234qwer"

    db_connections = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    engine = create_engine(db_connections, pool_recycle=600)
    conn = engine.connect()


    return engine, conn



def getfilesize(res):
	try:
		return int(res.headers['Content-Length'])
	except:
		return 0


def downloadfile(idx, url, filename):
	
	res = requests.get(url, verify=False)
	time.sleep(3)
	
	if res.status_code == 200:

		filesize = getfilesize(res)
		fileext = os.path.splitext(filename)[-1]
		outpath=f'./Report/downloadfile/downloadfile_{idx}{fileext}'

		retries = 2
		socket.setdefaulttimeout(60)
		
		while(retries > 0):
			try:
				urllib.request.urlretrieve(url, outpath)
				print("success")
				status = 1
				break
			except Exception as e:
				print("## ", e)
				retries = retries-1
				status = -1
				continue
		
		time.sleep(1.5)
		return status, filesize


	else:
		filesize = 0
		status = -1
		return status, filesize






if __name__ == "__main__":

	start_idx, end_idx = sys.argv[1], sys.argv[2]

	engine, conn = db_connection()

	set = 0

	while True:
		try:
			locking = "LOCK TABLE Report.pdfurl WRITE;"
			conn.execute(text(locking))
			conn.commit()
			
			query = f'select idx, url from Report.pdfurl where status=0 limit 1;'
			url_df = pd.read_sql_query(query, engine)
			
			if len(url_df)>=1 :
				for idx, url in zip(tqdm(url_df['idx']), url_df['url']):

					#status =2 , 다운로드 진행 중(점유)
					try:
						upd_query = f'UPDATE Report.pdfurl SET status = 2 WHERE idx={idx};'
						conn.execute(text(upd_query))
						conn.commit()
						
						unlocking = "UNLOCK TABLE;"
						conn.execute(text(unlocking))
						conn.commit()

					except Exception as e:
						print(e)
						continue


					try:
						file_name = url.split('/')[-1]
						status, filesize = downloadfile(idx, url, file_name)
						decode_filename = parse.unquote(file_name, encoding="utf-8")
						print("# org file name : ", file_name)
						print("# decode file name : ", decode_filename)
					

						if status == 1:
							dumpdf = pd.DataFrame([[idx, decode_filename, filesize]], columns = ['urlidx', 'file_name', 'file_size'])
							try:
								dumpdf.to_sql(name= 'pdfdownloadfileinfo', con=engine,  if_exists='append', index=False, method='multi')
							except IntegrityError:
								print('duplicated')
								pass

					except Exception as e:
						print(e)
						status = -1

					new_st = status
					try:
						upd_query = f'UPDATE Report.pdfurl SET status = {new_st} WHERE idx={idx};'
						conn.execute(text(upd_query))
						conn.commit()

					except Exception as e:
						continue


			else:
				break
				
		except OperationalError :
			try:
				conn.close()
				engine, conn = db_connection()
				pass
			except:
				break

		finally:
			conn.close()
		
	
	conn.close()



