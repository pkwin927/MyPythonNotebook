# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 16:25:04 2022

@author: JackeyChen陳威宇
"""

import os
import getpass
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import sys
import method.betradar_data as btd
import method.API_method as API_method
import requests

config_number = sys.argv[1]
# config_number = '1'
config_info = API_method.get_config_info(config_number = config_number)

port = config_info['job_api_port']
IP = API_method.get_IP()

## 輸入帳號密碼
# passwd = getpass.getpass("Please input domain password:")

## linux 環境需加
# passwd = "\""+str(passwd)+"\""
# print()
engine_list = btd.create_connect(config_number = config_number)

job_name = sys.argv[0]
# job_name = 'loss_data_redis'
control_info = btd.get_control_info(job_name,config_number = config_number)

control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)


# betradar_address = os.path.abspath(os.path.join(os.getcwd(), os.path.pardir)) + "\\"

## 需讓排程執行工作
def job_loss():
    
    today_Start =pd.to_datetime(datetime.datetime.now())
        
    # betradar_address = "D:/Users/Code/betradar"
    
    # print("###############AutoVbet Doing Now "+str(today_Start)+"###############")
          
    print("check loss "+str(today_Start))
    
    ## os.system() 執行命令，範例: os.system(python /home/bonnyhuang/schedule45U/ALLDays.py passwd)，可用python執行ALLDays程式
    r = requests.post(f'http://{IP}:{port}/api_control/start_job/error_recover?job_num=1')

    

    # os.system("python "+betradar_address+"error_recover.py")
    # print("check loss end")
    # os.system("python "+address+"ALLDays.py "+passwd)
    
    
    
    today_Stop =pd.to_datetime(datetime.datetime.today())
    print("check loss "+str(today_Stop)+' end')

    # print("###############AutoVbet is OVER "+str(today_Stop)+"############### ")

######################################################################################


# def job_betradar():
    
#     today_Start =pd.to_datetime(datetime.datetime.today())
        
#     # address = "D:/Users/Code/" 
#     print("###############job_betradar Doing Now "+str(today_Start)+"###############")
#     print("update_sports_data")
#     # os.system("python "+betradar_address+"update_sports_data.py")
#     r = requests.post('http://192.168.18.6:5000/api_control/start_job/update_sports_data')

#     # print("date_fixture_match")
    
#     # os.system("python "+betradar_address+"date_fixture_match.py")
    
#     print("betradar_to_maindb")
    
#     os.system("python "+betradar_address+"betradar_to_maindb.py")
    
#     today_Stop =pd.to_datetime(datetime.datetime.today())
    
#     print("###############job_betradar is OVER "+str(today_Stop)+"############### ")
    
if __name__ == "__main__":
    
    ## BlockingScheduler 務必要加，若不加程式運行時間較長的話，會有延時過久的錯誤
    scheduler = BlockingScheduler( job_defaults = {'misfire_grace_time':60*60} )
    
    ## add_job 添加排程執行時間點
    scheduler.add_job(job_loss,"interval",minutes = 1,misfire_grace_time=60*60)
    
    # scheduler.add_job(job_betradar,"cron",day_of_week = "mon-sun", hour = 1, minute = 0,misfire_grace_time=60*60)

    
    ## 停止機制
    try:
        
        scheduler.start()
        
    except(KeyboardInterrupt, SystemExit):
        
        scheduler.shutdown(wait = False)
        
        print('Exit The Job!')
