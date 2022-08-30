# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 11:09:30 2022

@author: JackeyChen陳威宇
"""

import method.log_config as log_config
log_handler,logging = log_config.create_logger('sync_code')

try:
    import method.betradar_data as btd
    import method.API_method as API_method
    import pandas as pd
    import sys
    import time
    from sqlalchemy.sql import text as sa_text

    
    config_number = sys.argv[1]
    config_info = API_method.get_config_info(config_number = config_number)
    env_name = config_info['env_name']
    # config_number = '2'
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    local_ip = API_method.get_IP()
    
    job_name = sys.argv[0]
    control_info = btd.get_control_info(job_name,config_number = config_number)
    control_IP = control_info['IP'][0]
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "sync_code.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    
    # local_ip = '192.168.254.134'
    user_info = API_method.get_user_info(local_ip, engine)
    manager_info = API_method.get_position_info(engine = engine_list[0], position = 'manager')
    source_path = manager_info['Path'][0]
    local_path = user_info['Path'][0]
    local_path = local_path.rsplit('/',1)[0]
    # local_path = "D:\\Users\\Code\\betradar_linux"
    API_method.control_api_close_api(local_ip,config_number)
    print("start sync")
    API_method.ssh_scp_files(ssh_host = manager_info['IP'][0],
                              ssh_user = manager_info['UserName'][0],
                              ssh_password = API_method.decode_password(manager_info['Password'][0]),
                              source_path = source_path,
                              local_path = local_path)
    # time.sleep(2)
    print('Open')
    print(config_number)
    API_method.control_api_open_api(local_ip,config_number)
    time.sleep(3)
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "sync_code.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    
    # IP = user_info.loc[i,'IP']
    # user_name = user_DF.loc[i,'UserName']
    
    # API_method.control_api_close_api(local_ip,config_number)
    # time.sleep(1)
    # API_method.control_api_open_api(local_ip,config_number)
except:
    try:
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()
    except:
        log_handler.exception(f"""#####################Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()
