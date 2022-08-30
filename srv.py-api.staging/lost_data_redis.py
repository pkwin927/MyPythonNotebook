# -*- coding: utf-8 -*-
"""
Created on Tue Apr 12 14:33:25 2022

@author: JackeyChen陳威宇
"""


# import redis
import method.log_config as log_config
log_handler,logging = log_config.create_logger('lost_data_redis')
try:
    import method.betradar_data as btd
    import datetime as dt
    import pandas as pd
    import numpy as np
    import os
    import sys
    import configparser
    import method.single_API_method as API_method
    from sqlalchemy.sql import text as sa_text

    config_number = sys.argv[1]
    config_info = API_method.get_config_info(config_number = config_number)
    env_name = config_info['env_name']
    # config_number = '2'
    engine_list = btd.create_connect(config_number = config_number)
    # config = configparser.ConfigParser()
    database_user = config_info['database_user']
    # game_database_name = config_info['game_database_name']
    # engine_list = btd.create_connect()
    job_name = sys.argv[0]
    # print(job_name)
    # job_name = 'loss_data_redis'
    control_info = btd.get_control_info(job_name,config_number = config_number)
    
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    while True:
        try:
            print('#################################################')
            print('start')
            # redis_conn = btd.create_redis(config_number = config_number)
    
            engine_list = btd.create_connect(config_number = config_number)
            
            game_league_ID = pd.read_sql(f'select LeagueID from game_league where LanguageCode = "zh-cn" ', con =engine_list[1])
            
            game_league_ID = game_league_ID['LeagueID'].tolist()
            
            while True:
                try:
                    # raise ValueError
                    redis_conn = btd.create_redis(config_number = config_number)
                    data_tuple = redis_conn.blpop('lost_event')
                    if data_tuple[1] == 'New member':
                        continue
                    # data_tuple = ('lost_event', 'sr:match:35001077:1')
                    redis_time = pd.to_datetime(dt.datetime.now())
                    # print(data_tuple)
                    redis_input = [data_tuple[1],'Doing',redis_time,2]
                    redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                    redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                    # redis_input.to_sql(name = 'game_input_history',con = engine_list[3],if_exists = 'append',index = False)
                    # print('OK')        
                    # btd.dispose_engine(engine_list)
                    
            
                    betradar_address = os.path.abspath(os.getcwd()) + "/"
                    
                    # path = betradar_address +'Error.txt'
                    a_day = dt.timedelta(days = 1)
                    creator = database_user
                    # match_id = 'sr:match:34858233:0'
                    match_id = data_tuple[1]
                    today = pd.to_datetime(dt.datetime.now())
                    if len(match_id.rsplit(':')) == 3:
                        ID_list = match_id.rsplit(':',1)
                        ID_list.extend('0')
                    elif len(match_id.rsplit(':')) == 4:
                        
                        ID_list = match_id.rsplit(':',2)
                        
                    else:
                        redis_time = pd.to_datetime(dt.datetime.now())
                        redis_input = [data_tuple[1],'error: not a legally ID',redis_time,0]
                        redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                        redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                        # sql_commend_str = f"""update game_input_history
                        #                      set Recover = 0
                        #                      where GameID = "{match_id}" and Recover = 3 """
                        # engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
                        
                        btd.dispose_engine(engine_list)
                        continue
                    if ID_list[0] == 'sr:match':
                        match_data = btd.get_fixture_match(series = [ID_list[1]])
                        # match_data['start_time']
                        # match_data.to_sql(name = 'sport_events_fixture_sr:match',con = engine_list[1],if_exists = 'append',index = False)
                        match_data.to_sql(name = 'sport_events_fixture_sr_match',con = engine_list[0],if_exists = 'append',index = False)
            
                        match_game_team_list, match_game_team, match_game_match = btd.get_game_match_data(targetDF = match_data,config_number = config_number)
                        
                        
                        TeamNameNum = 0
                        for team_str in match_game_team["TeamName"]:
                            if btd.string_mapping(team_str) == True:
                                
                                TeamNameNum += 1
                        if TeamNameNum > 0:
                            redis_time = pd.to_datetime(dt.datetime.now())
                            redis_input = [match_id,f'error: ({team_str}),TeamName error',redis_time,1]
                            redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                            redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                            # sql_commend_str = f"""update game_input_history
                            #                      set Recover = 0
                            #                      where GameID = "{match_id}" and Recover = 3 """
                            # engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
                            
                            btd.dispose_engine(engine_list)
                            continue
                        
                        match_game_match['Producer'] = int(ID_list[2])
                        btd.update_duplicate_to_sql(insert_DF = match_game_team_list, table_name = 'game_team_list', engine = engine_list[1])
                        btd.update_duplicate_to_sql(insert_DF = match_game_team, table_name = 'game_team', engine = engine_list[1])
                        btd.update_duplicate_to_sql(insert_DF = match_game_match, table_name = 'game_match', engine = engine_list[1])
                        
                        if match_game_match['LeagueID'].unique()[0] in game_league_ID:
                            pass
                        else:
                            new_game_league = btd.update_game_league(engine_list,config_number = config_number)
    
                            new_game_league_ID = new_game_league['LeagueID'].drop_duplicates().tolist()
                            
                            game_league_ID.extend(new_game_league_ID)
                            
                        # match_game_team_list.to_sql(name = 'game_team_list',con = engine_list[3],if_exists = 'append',index = False)
                        # match_game_team.to_sql(name = 'game_team',con = engine_list[3],if_exists = 'append',index = False)
                        # match_game_match.to_sql(name = 'game_match',con = engine_list[3],if_exists = 'append',index = False)
                        
                        # match_game_team_list.to_sql(name = 'game_team_list',con = engine_list[2],if_exists = 'append',index = False)
                        # match_game_team.to_sql(name = 'game_team',con = engine_list[2],if_exists = 'append',index = False)
                        # match_game_match.to_sql(name = 'game_match',con = engine_list[2],if_exists = 'append',index = False)
                        redis_time = pd.to_datetime(dt.datetime.now())
                        redis_input = [data_tuple[1],'complete',redis_time,0]
                        redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                        redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                        
                        sql_commend_str = """update game_input_history
                                             set Recover = 0
                                             where GameID = """ +'"' +data_tuple[1]+'"' + """;"""
                        
                        # redis_input.to_sql(name = 'game_input_history',con = engine_list[3],if_exists = 'append',index = False)
                        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
                        btd.dispose_engine(engine_list)
                        
                    elif ID_list[0] == 'sr:season':
                        match_data = btd.get_fixture_season(series = [ID_list[1]])
                        if match_data.shape[0] == 0:
                            redis_time = pd.to_datetime(dt.datetime.now())
                            redis_input = [data_tuple[1],'warnning: no team for the season',redis_time,0]
                            redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                            redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                            sql_commend_str = """update game_input_history
                                                 set Recover = 0
                                                 where GameID = """ +'"' +match_id+'"' + """;"""
                            # redis_input.to_sql(name = 'game_input_history',con = engine_list[3],if_exists = 'append',index = False)
                            btd.dispose_engine(engine_list)
                            continue
                        # match_data.to_sql(name = 'sport_events_fixture_sr:season',con = engine_list[1],if_exists = 'append',index = False)
                        match_data.to_sql(name = 'sport_events_fixture_sr_season',con = engine_list[0],if_exists = 'append',index = False)
                        
                        match_game_team_list, match_game_team, match_game_match = btd.get_game_season_data(targetDF = match_data,config_number = config_number)
                    
                        
                        # btd.update_duplicate_to_sql(insert_DF = match_game_team_list, table_name = 'game_team_list', engine = engine_list[3])
                        # btd.update_duplicate_to_sql(insert_DF = match_game_team, table_name = 'game_team', engine = engine_list[3])
                        # btd.update_duplicate_to_sql(insert_DF = match_game_match, table_name = 'game_match', engine = engine_list[3])
                        match_game_match['Producer'] = int(ID_list[2])
                        btd.update_duplicate_to_sql(insert_DF = match_game_team_list, table_name = 'game_team_list', engine = engine_list[1])
                        btd.update_duplicate_to_sql(insert_DF = match_game_team, table_name = 'game_team', engine = engine_list[1])
                        btd.update_duplicate_to_sql(insert_DF = match_game_match, table_name = 'game_match', engine = engine_list[1])
                        
                        if match_game_match['LeagueID'].unique()[0] in game_league_ID:
                            pass
                        else:
                            new_game_league = btd.update_game_league(engine_list,config_number = config_number)
                            
                            new_game_league_ID = new_game_league['LeagueID'].drop_duplicates().tolist()
                            
                            game_league_ID.extend(new_game_league_ID)
                        
                        # match_game_team_list.to_sql(name = 'game_team_list',con = engine_list[3],if_exists = 'append',index = False)
                        # match_game_team.to_sql(name = 'game_team',con = engine_list[3],if_exists = 'append',index = False)
                        # match_game_match.to_sql(name = 'game_match',con = engine_list[3],if_exists = 'append',index = False)
                        
                        # match_game_team_list.to_sql(name = 'game_team_list',con = engine_list[2],if_exists = 'append',index = False)
                        # match_game_team.to_sql(name = 'game_team',con = engine_list[2],if_exists = 'append',index = False)
                        # match_game_match.to_sql(name = 'game_match',con = engine_list[2],if_exists = 'append',index = False)
                        redis_time = pd.to_datetime(dt.datetime.now())
                        redis_input = [data_tuple[1],'complete',redis_time,0]
                        redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                        redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                        # redis_input.to_sql(name = 'game_input_history',con = engine_list[3],if_exists = 'append',index = False)
                        sql_commend_str = """update game_input_history
                                             set Recover = 0
                                             where GameID = """ +'"' +data_tuple[1]+'"' + """;"""
                        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
                        btd.dispose_engine(engine_list)
                    else:
                        redis_time = pd.to_datetime(dt.datetime.now())
                        redis_input = [match_id,'error: ID incorrect format '+str(ID_list)+' ,please check.',redis_time,1]
                        redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                        redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                        # redis_input.to_sql(name = 'game_input_history',con = engine_list[3],if_exists = 'append',index = False)
                        # sql_commend_str = f"""update game_input_history
                        #                      set Recover = 0
                        #                      where GameID = "{match_id}" and Recover = 3 """
                        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
                        
                        btd.dispose_engine(engine_list)
                        # raise ValueError(ID_list[1] +'incorrect format，just in sr:match or sr:season')
                        # print(ID_list[1] +'incorrect format，just in sr:match or sr:season')
                except Exception as e:
                    redis_time = pd.to_datetime(dt.datetime.now())
                    redis_input = [match_id,'error: '+str(e),redis_time,1]
                    redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                    redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                    # redis_input.to_sql(name = 'game_input_history',con = engine_list[3],if_exists = 'append',index = False)
                    # sql_commend_str = f"""update game_input_history
                    #                      set Recover = 0
                    #                      where GameID = "{match_id}" and Recover = 3 """
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
            continue
except:
    try:
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()  
    except:
        
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()  
