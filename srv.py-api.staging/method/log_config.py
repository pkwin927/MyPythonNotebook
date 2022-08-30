# -*- coding: utf-8 -*-
"""
Created on Tue Aug  9 15:17:03 2022

@author: JackeyChen陳威宇
"""

import logging
import datetime as dt
import os

def create_logger(log_file_name):
    
    # fmt_str = '%(asctime)s[level-%(levelname)s][%(name)s]:%(message)s'
    
    FORMAT = """%(message)s %(asctime)s %(levelname)s """
    log_date = dt.datetime.now().strftime("%Y-%m-%d.log")
    # log_date = f'{log_time}.log'
    try:
        filehandle = logging.FileHandler(f'./log/{log_file_name}/{log_date}')
    except:
        os.mkdir(f"./log/{log_file_name}")
        filehandle = logging.FileHandler(f'./log/{log_file_name}/{log_date}')
    formatter = logging.Formatter(FORMAT)
    
    filehandle.setFormatter(formatter)
    
    logging.basicConfig(level = logging.INFO,filemode='a',encoding = 'utf-8') 
    log_handler = logging.getLogger('')
    log_handler.addHandler(filehandle)
    
    return log_handler,logging