# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 17:14:35 2017

@author: jackey.chen
"""

import os
import getpass
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime

passwd = getpass.getpass("Please input domain password:")

# Linux need add 
#passwd = "\""+str(passwd)+"\""

def job():
    
    today_Start =pd.to_datetime(datetime.datetime.today())

    print("###############Doing Now "+str(today_Start)+"###############")
          
    print("1")
    
    os.system("python C:/Users/jackey.chen/schedule/ALLDays.py "+passwd)
    

    today_Stop =pd.to_datetime(datetime.datetime.today())


    print("###############Job is OVER "+str(today_Stop)+"############### ")
          
#def job2():
#    
#    os.system("python C:/Users/jackey.chen/schedule/muCalcCustomerCalendar.py "+passwd)
#    
#
#
#    print("###############Job2 is OVER############### ")


if __name__ == "__main__":
    
    scheduler = BlockingScheduler( job_defaults = {'misfire_grace_time':60*60} )
    scheduler.add_job(job,"cron",day_of_week = "mon-sun", hour = 5, minute = 1,misfire_grace_time=60*60)
#    scheduler.add_job(job,"cron",day_of_week = "mon-sun", hour = 12, minute = 6,misfire_grace_time=60*60)
#    scheduler.add_job(job,"interval",seconds = 30)
#    scheduler.add_job(job2,"cron",day_of_week = "mon-sun", hour = 8, minute = 00, misfire_grace_time = 60*60)
    
    try:
        
        scheduler.start()
        
    except (KeyboardInterrupt,SystemExit):
        
        scheduler.shutdown()
        
        print('Exit The Job!')

    










