# -*- coding: utf-8 -*-
"""
Created on Fri Aug 19 10:08:51 2022

@author: JackeyChen陳威宇
"""
import method.log_config as log_config
log_file_name = 'save_game_input_history'

log_handler,logging = log_config.create_logger(log_file_name)

try:
    import pandas as pd
    import method.single_API_method as API_method
    import method.betradar_data as btd
    import sys
    import datetime as dt
    from sqlalchemy.sql import text as sa_text
    
    
    config_number = sys.argv[1]
    job_name = sys.argv[0]
    # config_number = '2'
    engine_list = btd.create_connect(config_number = config_number)
    control_info = btd.get_control_info(job_name,config_number = config_number)
    
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    IP = API_method.get_IP()
    config_info = API_method.get_config_info(config_number = config_number)
    env_name = config_info['env_name']
    
    today = pd.to_datetime(dt.datetime.now()) 
    day56 = dt.timedelta(days = 56)
    
    delete_date = today - day56
    delete_date = delete_date.strftime('%Y-%m-%d')
    input_history_DF = pd.read_sql(f"""SELECT GameID, max(CreateTime) as max_CreateTime,max(Recover) as Recover FROM betradardb.game_input_history group by GameID """,con = engine_list[0])
    
    delete_gameID_list = list(input_history_DF.loc[(input_history_DF['max_CreateTime'] <= delete_date) & (input_history_DF['Recover'] == 0),'GameID'])
    
    if len(delete_gameID_list) != 0:
    
        for GameID in delete_gameID_list:
            # break
            print(GameID)
            try:
                GameID_history_DF = pd.read_sql(f"""SELECT * FROM betradardb.game_input_history where GameID = "{GameID}" """,con = engine_list[0])
                
                GameID_history_DF.to_sql('game_input_history_log',con = engine_list[0],if_exists = 'append',index = False)
                
                sql_cmd = f"""delete from game_input_history where GameID = "{GameID}" """
                engine_list[0].execute(sa_text(sql_cmd).execution_options(autocommit=True))
            except:
                continue
            
        sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{IP}" and Job = "{log_file_name}.py";"""
        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    
        btd.dispose_engine(engine_list)
    else:
        sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{IP}" and Job = "{log_file_name}.py";"""
        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    
        btd.dispose_engine(engine_list)
        
except:
    try:
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()  
    except:
        
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()  