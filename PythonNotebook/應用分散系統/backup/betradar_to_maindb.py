# -*- coding: utf-8 -*-
"""
Created on Fri Mar  4 09:58:44 2022

@author: JackeyChen陳威宇
"""

import requests
import pandas as pd
import json
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
import xmltodict
import time
import datetime as dt
import numpy as np
import warnings
import method.betradar_data as btd
import method.API_method as API_method
import traceback
import sys
from sqlalchemy.sql import text as sa_text


config_number = sys.argv[1]
# config_number = '2'
warnings.filterwarnings('ignore')
# pd.set_option('display.max_rows',None)

engine_list = btd.create_connect(config_number = config_number)
engine_betradar = engine_list[0]
engine_main = engine_list[1]
config_info = API_method.get_config_info(config_number = config_number)
job_name = sys.argv[0]

control_info = btd.get_control_info(job_name,config_number = config_number)

control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)

local_ip = API_method.get_IP()

today = pd.to_datetime(dt.datetime.now())
a_day = dt.timedelta(days = 1)
creator = config_info['database_user']
# path = 'D:/Users/Code/betradar/Error.txt'
# today_date = pd.to_datetime(dt.datetime.today().date()) 
# today_date = today_date.strftime('%Y/%m/%d')
#################################################################
# today = pd.to_datetime('2022/3/3')
# a_day = dt.timedelta(days = 1)
# today_date = pd.to_datetime('2022/3/6')
today_date = today.strftime('%Y/%m/%d')
seven_day = today + 8*a_day
seven_date = seven_day.strftime('%Y/%m/%d')

############################################################################
## game_kind
try:
    
    sport_DF = pd.read_sql('select * from sports_all',con = engine_betradar)
    
    sport_DF = btd.get_game_kind_data(targetDF = sport_DF,engine_main = engine_main, config_number = config_number)
    # league_DF.columns
    sport_DF.to_sql(name = 'game_kind',con = engine_main,if_exists = 'append',index = False)
    # sport_DF.to_sql(name = 'game_kind',con = engine_test,if_exists = 'append',index = False)
    
    ############################################################################
    ##game_region
    
    categories_DF = pd.read_sql('select * from sports_categories',con = engine_betradar)
    
    categories_DF = btd.get_game_region_data(targetDF = categories_DF,engine_main = engine_main, config_number = config_number)
    
    categories_DF.to_sql(name = 'game_region',con = engine_main,if_exists = 'append',index = False)
    # categories_DF.to_sql(name = 'game_region',con = engine_test,if_exists = 'append',index = False)
    
    ############################################################################
    ##game_league
    league_DF = pd.read_sql('select * from tournaments_all',con = engine_betradar)
    
    league_DF = btd.get_game_league_data(targetDF = league_DF,engine_main = engine_main, config_number = config_number)
    
    league_DF.to_sql(name = 'game_league',con = engine_main,if_exists = 'append',index = False)
    # league_DF.to_sql(name = 'game_league',con = engine_test,if_exists = 'append',index = False)
    
    ############################################################################
    ############################################################################
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and Job = "betradar_to_maindb.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))

        
    btd.dispose_engine(engine_list)
except Exception:
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and Job = "betradar_to_maindb.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))

    btd.dispose_engine(engine_list)