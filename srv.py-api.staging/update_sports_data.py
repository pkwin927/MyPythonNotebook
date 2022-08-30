# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 11:14:44 2022

@author: JackeyChen陳威宇
"""
import method.log_config as log_config
log_handler,logging = log_config.create_logger('update_sport_data')
try:
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
    from sqlalchemy.sql import text as sa_text
    import traceback
    import method.betradar_data as btd
    import sys
    import method.single_API_method as API_method
    warnings.filterwarnings('ignore')

    config_number = sys.argv[1]
    config_info = API_method.get_config_info(config_number = config_number)
    env_name = config_info['env_name']
    
    engine_list = btd.create_connect(config_number = config_number)
    
    today = pd.to_datetime(dt.datetime.now())
    a_day = dt.timedelta(days = 1)
    
    creator = config_info['database_user']
    
    job_name = sys.argv[0]

    control_info = btd.get_control_info(job_name,config_number = config_number)
    
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    local_ip = API_method.get_IP()
##########################################################################
## game_sports_all
# try:
    engine = engine_list[0]

    connect = engine.connect()
    engine.execute(sa_text('''TRUNCATE TABLE sports_all''').execution_options(autocommit=True))
    
    url = 'https://stgapi.betradar.com/v1/sports/zh/sports.xml'
    
    headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
    response = requests.get(url,headers = headers)
    DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
    DF['LanguageCode'] = 'zh-cn'
    DF.to_sql(name = 'sports_all',con = engine,if_exists = 'append',index = False)
    #########################################################################################
    url = 'https://stgapi.betradar.com/v1/sports/en/sports.xml'
    
    headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
    response = requests.get(url,headers = headers)
    DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
    DF['LanguageCode'] = 'en-us'
    DF.to_sql(name = 'sports_all',con = engine,if_exists = 'append',index = False)
    sport_number_list = list(DF['id'].str.rsplit(':',1, expand = True)[1].astype(int))
    sport_number_list.sort()
    ##########################################################################
    ################################################################################################
    ## tournaments_all
    engine.execute(sa_text('''TRUNCATE TABLE tournaments_all''').execution_options(autocommit=True))

    
    url = 'https://stgapi.betradar.com/v1/sports/zh/tournaments.xml'
    
    headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
    response = requests.get(url,headers = headers)
    obj = xmltodict.parse(response.text)
    aaa = json.dumps(obj)
    aaa = json.loads(aaa)
    bbb = aaa['tournaments']['tournament']
    result = pd.DataFrame(bbb)
    result_columns = []
    for i in range(len(result.columns)):
        result_columns.append(result.columns[i].replace('@',''))
    result.columns = result_columns
    
    for i in result.columns:
        if i in ['scheduled','scheduled_end']:
            pass
        else:
            result[i] = result[i].astype('str')
    result['LanguageCode'] = 'zh-cn'

    result.to_sql(name = 'tournaments_all',con = engine,if_exists = 'append',index = False)
    
    ################################################################################################
    url = 'https://stgapi.betradar.com/v1/sports/en/tournaments.xml'
    
    headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
    response = requests.get(url,headers = headers)
    obj = xmltodict.parse(response.text)
    aaa = json.dumps(obj)
    aaa = json.loads(aaa)
    bbb = aaa['tournaments']['tournament']
    result = pd.DataFrame(bbb)
    result_columns = []
    for i in range(len(result.columns)):
        result_columns.append(result.columns[i].replace('@',''))
    result.columns = result_columns
    
    for i in result.columns:
        if i in ['scheduled','scheduled_end']:
            pass
        else:
            result[i] = result[i].astype('str')

    result['LanguageCode'] = 'en-us'
    result.to_sql(name = 'tournaments_all',con = engine,if_exists = 'append',index = False)
    
    ################################################################################################
    ################################################################################################
    ## sports_categories
    
    engine.execute(sa_text('''TRUNCATE TABLE sports_categories''').execution_options(autocommit=True))
    for sports_number in sport_number_list:
        # print(sports_number)
        url = 'https://stgapi.betradar.com/v1/sports/zh/sports/sr:sport:'+str(sports_number)+'/categories.xml'
        
        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        obj = xmltodict.parse(response.text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        try:
            bbb = aaa['sport_categories']['categories']['category']
        except:
            continue
            
        if isinstance(bbb,dict):
            result = pd.DataFrame([bbb])
        else:
            result = pd.DataFrame(bbb)
        result['sport'] = str(aaa['sport_categories']['sport'])
        
        result_columns = []
        for i in range(len(result.columns)):
            result_columns.append(result.columns[i].replace('@',''))
        result.columns = result_columns
        
        result['sport'] = result['sport'].astype('str')
        
        result['LanguageCode'] = 'zh-cn'
        if 'country_code' in result.columns:
            result = result[['LanguageCode','sport','id','name','country_code']]
        else:
            result['country_code'] = np.nan
        result.to_sql(name = 'sports_categories',con = engine,if_exists = 'append',index = False)
        
        ########################################################################################################
        
        url = 'https://stgapi.betradar.com/v1/sports/en/sports/sr:sport:'+str(sports_number)+'/categories.xml'
        
        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        obj = xmltodict.parse(response.text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        bbb = aaa['sport_categories']['categories']['category']
        if isinstance(bbb,dict):
            result = pd.DataFrame([bbb])
        else:
            result = pd.DataFrame(bbb)
        result['sport'] = str(aaa['sport_categories']['sport'])
        
        result_columns = []
        for i in range(len(result.columns)):
            result_columns.append(result.columns[i].replace('@',''))
        result.columns = result_columns
        
        
        result['sport'] = result['sport'].astype('str')
        
        result['LanguageCode'] = 'en-us'
        if 'country_code' in result.columns:
            result = result[['LanguageCode','sport','id','name','country_code']]
        else:
            result['country_code'] = np.nan
        
        result.to_sql(name = 'sports_categories',con = engine,if_exists = 'append',index = False)
    
    ################################################################################################
    ################################################################################################
    # ## descriptions_markets
    # engine.execute(sa_text('''TRUNCATE TABLE descriptions_markets''').execution_options(autocommit=True))
    
    # url = 'https://stgapi.betradar.com/v1/descriptions/zh/markets.xml?include_mappings=true'
    
    # headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
    # response = requests.get(url,headers = headers)
    # # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
    # obj = xmltodict.parse(response.text)
    # aaa = json.dumps(obj)
    # aaa = json.loads(aaa)
    # bbb = aaa['market_descriptions']['market']
    # result = pd.DataFrame(bbb)
    # result_columns = []
    # for i in range(len(result.columns)):
    #     result_columns.append(result.columns[i].replace('@',''))
    # result.columns = result_columns
    
    # for i in result.columns:
    #     if i in ['scheduled','scheduled_end','next_live_time','update_time']:
    #         pass
    #     else:
    #         result[i] = result[i].astype('str')
    
    # result['LanguageCode'] = 'zh-cn'
    # result['outcomes'] = result['outcomes'].str.replace("'",'"')
    # result['specifiers'] = result['specifiers'].str.replace("'",'"')
    # result['mappings'] = result['mappings'].str.replace("'",'"')
    # result['attributes'] = result['attributes'].str.replace("'",'"')
    
    # result.to_sql(name = 'descriptions_markets',con = engine,if_exists = 'append',index = False) 
                    
    #######################################################################################################################             
    
    # url = 'https://stgapi.betradar.com/v1/descriptions/en/markets.xml?include_mappings=true'
    
    # headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
    # response = requests.get(url,headers = headers)
    # # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
    # obj = xmltodict.parse(response.text)
    # aaa = json.dumps(obj)
    # aaa = json.loads(aaa)
    # bbb = aaa['market_descriptions']['market']
    # result = pd.DataFrame(bbb)
    
    # result_columns = []
    # for i in range(len(result.columns)):
    #     result_columns.append(result.columns[i].replace('@',''))
    # result.columns = result_columns
    
    # for i in result.columns:
    #     if i in ['scheduled','scheduled_end','next_live_time','update_time']:
    #         pass
    #     else:
    #         result[i] = result[i].astype('str')
    # result['LanguageCode'] = 'en-us'
    # result['outcomes'] = result['outcomes'].str.replace("'",'"')
    # result['specifiers'] = result['specifiers'].str.replace("'",'"')
    # result['mappings'] = result['mappings'].str.replace("'",'"')
    # result['attributes'] = result['attributes'].str.replace("'",'"')
    
    # result.to_sql(name = 'descriptions_markets',con = engine,if_exists = 'append',index = False)
    ################################################################################################
    ################################################################################################
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and Job = "update_sports_data.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))

    connect.close()
    engine.dispose()
    
except Exception:
    try:
        sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and Job = "update_sports_data.py";"""
        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
        connect.close()
        engine.dispose()
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()
    except:
        try:
            log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
            log_handler.removeHandler(log_handler.handlers[1])
            logging.shutdown()
        except:
            log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
            log_handler.removeHandler(log_handler.handlers[1])
            logging.shutdown()