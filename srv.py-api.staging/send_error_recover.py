# -*- coding: utf-8 -*-
"""
Created on Mon May  9 09:54:07 2022

@author: JackeyChen陳威宇
"""

import method.log_config as log_config
log_file_name = 'send_error_recover'

log_handler,logging = log_config.create_logger(log_file_name)
try:

    import method.betradar_data as btd
    import datetime as dt
    import pandas as pd
    import numpy as np
    import os
    from sqlalchemy.sql import text as sa_text
    import sys
    import method.single_API_method as API_method
    import redis

    
# import configparser

    config_number = sys.argv[1]
    config_info = API_method.get_config_info(config_number = config_number)
    
    env_name = config_info['env_name']
    engine_list = btd.create_connect(config_number = config_number)
    
    # config_number = '2'
    job_name = sys.argv[0]
    # job_name = 'error_recover.py'
    control_info = btd.get_control_info(job_name,config_number = config_number)
    control_IP = control_info['IP'][0]
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "{log_file_name}.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    
    redis_conn = btd.create_redis(config_number = config_number)
    
    
    # config_info = configparser.ConfigParser(config_number = config_number)

    engine = engine_list[1]
    
    DF_error = pd.read_sql("""select * from game_input_history
                        where Recover = 1""",con = engine_list[0])
    
    
    a_day = dt.timedelta(days = 1)
    creator = config_info['database_user']
    
    today = pd.to_datetime(dt.datetime.now())
    
    sql_commend_str = """use maindb;"""
                         
    engine.execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    
    sql_commend_sre = """SET SQL_SAFE_UPDATES = 0;"""
    
    engine.execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    # engine.dispose
    game_league_ID = pd.read_sql('select LeagueID from maindb.game_league where LanguageCode = "zh-cn" ', con =engine_list[1])
    
    game_league_ID = game_league_ID['LeagueID'].tolist()
    
    error_id = DF_error['GameID'].drop_duplicates()
    if error_id.shape[0] != 0:
        
        for match_id in error_id:
            print(match_id)
            # match_id = 'sr:match:34240775:1'
            try:
                
                if len(match_id.rsplit(':')) == 3:
                    ID_list = match_id.rsplit(':',1)
                    ID_list.extend('0')
                elif len(match_id.rsplit(':')) == 4:
                    
                    ID_list = match_id.rsplit(':',2)
                else:
                    redis_time = pd.to_datetime(dt.datetime.now())
                    redis_input = [match_id,'error: not a legally ID',redis_time,0]
                    redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                    redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                
                check_DF = pd.read_sql(f"""select * from game_input_history
                                    where GameID = "{match_id}" and Recover = 3""",con = engine_list[0])
                if check_DF.shape[0] == 0:
    
                    redis_conn = btd.create_redis(config_number = config_number)
                    # redis_conn = redis.Redis(host='localhost', port=6379, db=0)
                    redis_conn.lpush("lost_event", match_id)
    
                    redis_time = pd.to_datetime(dt.datetime.now())
                    redis_input = [match_id,'error_recover',redis_time,3]
                    redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                    redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                    
                    # sql_commend_str = """update game_input_history
                    #                      set Recover = 0
                    #                      where GameID = """ +'"' +match_id+'"' + """;"""
                    
                    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
            
                    btd.dispose_engine(engine_list)
                else:
                    continue
            except Exception as e:
                redis_time = pd.to_datetime(dt.datetime.now())
                redis_input = [match_id,'error: '+str(e),redis_time,1]
                redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
        
                btd.dispose_engine(engine_list)
                
                # print('error recover endding')
    
        sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "error_recover.py";"""
        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    
        btd.dispose_engine(engine_list)
    else:
        sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "error_recover.py";"""
        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
        btd.dispose_engine(engine_list)
        
except:
    try:
        sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "error_recover.py";"""
        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
        btd.dispose_engine(engine_list)
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")

    except:
        
        log_handler.exception(f"""#####################Catch an exception.#####################""")

    
    
    
    
    
    
    
    
    
    
    