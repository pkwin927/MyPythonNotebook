# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 14:19:29 2022

@author: JackeyChen陳威宇
"""
import method.log_config as log_config
log_file_name = 'single_server'
log_handler,logging = log_config.create_logger(log_file_name)

try:
    import requests
    import method.betradar_data as btd
    import method.API_method as API_method
    import sys
    import os
    import time
    # config_number = '2'
    config_number = sys.argv[1]
    ExecStart = f"/bin/bash -c 'source /opt/miniconda3/bin/activate company && python /opt/srv.betradar-py.staging/app.py {config_number} &'"
    # config_number = '2'
    IP = API_method.get_IP()
    # IP = '192.168.254.134'
    engine_list = btd.create_connect(config_number = config_number)
    
    config_info = API_method.get_config_info(config_number = config_number)
    app_port = config_info['job_api_port']
    env_name = config_info['env_name']
    # print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    # try:
    user_info = API_method.get_user_info(IP, engine_list[0])
    if user_info.shape[0] == 0:
        r = os.system(ExecStart)
    else:
        user_position = user_info['Position'][0]
        API_method.control_api_close_api(IP, config_number)
        time.sleep(1)
        API_method.control_api_open_api(IP, config_number)
    # except:
        time.sleep(3)
        # total_restart = 0
        API_method.app_stop_all_job(config_number = config_number)
        API_method.app_start_job(job_name = 'lost_data_redis', config_number = config_number)
        API_method.app_start_job(job_name = 'betradar_schedule', config_number = config_number)
        
        # while True:
        #     r = requests.get(f'http://{IP}:{app_port}/')
            
        #     if r.status_code == 200:
                
        #         requests.delete(f'http://{IP}:{app_port}/api_control/stop_all_job')
                
        #         requests.post(f'http://{IP}:{app_port}/api_control/start_job/lost_data_redis?job_num=1')
                
        #         requests.post(f'http://{IP}:{app_port}/api_control/start_job/betradar_schedule?job_num=1')
                
        #     else:
        #         time.sleep(3)
        #         total_restart+=1
        #         if total_restart == 11:
        #             break
        #         else:
        #             continue
except Exception as e:
    try:
        # log_handler.removeHandler(log_handler.handlers[1])
        # log_handler,logging = log_config.create_logger('app')
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        # log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()
        # return {'result':0, "outString":str(e)}
    except:
        # log_handler.removeHandler(log_handler.handlers[1])
        # log_handler,logging = log_config.create_logger('app')
        log_handler.exception(f"""#####################Catch an exception.#####################""")
        # log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()

# if user_info.shape[0] == 0:
    
    # os.system("/bin/bash -c 'source /opt/miniconda3/bin/activate company && python /opt/srv.betradar-py.staging/app.py 2' ")
    
    # r = requests.get('http://192.168.1.243:5011/')


