# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 16:25:04 2022

@author: JackeyChen陳威宇
"""
import method.log_config as log_config
log_file_name = 'api_schedule'
log_handler,logging = log_config.create_logger(log_file_name)

try:
    import os
    import getpass
    import pandas as pd
    from apscheduler.schedulers.blocking import BlockingScheduler
    from sqlalchemy.sql import text as sa_text
    import datetime
    import sys
    import method.betradar_data as btd
    import method.single_API_method as API_method
    import requests
    
    
    config_number = sys.argv[1]
    # config_number = '1'
    config_info = API_method.get_config_info(config_number = config_number)
    env_name = config_info['env_name']
   
    engine_list = btd.create_connect(config_number = config_number)
    
    job_name = sys.argv[0]
    # job_name = 'loss_data_redis'
    control_info = btd.get_control_info(job_name,config_number = config_number)
    control_IP = control_info['IP'][0]
    control_creator = control_info['Creator'][0]
    # sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "{log_file_name}.py";"""
    # engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    btd.dispose_engine(engine_list)
    # betradar_address = os.path.abspath(os.path.join(os.getcwd(), os.path.pardir)) + "\\"
    
    ## 需讓排程執行工作
    # def job_loss():
    #     # engine_list = btd.create_connect(config_number = config_number)
    #     global log_handler,logging
    #     log_handler.removeHandler(log_handler.handlers[1])
    #     log_handler,logging = log_config.create_logger(log_file_name)
    #     try:
    #         config_info = API_method.get_config_info(config_number = config_number)
    #         IP = API_method.get_IP()
    #         # port = config_info['job_api_port']
    #         # r = requests.post(f'http://{IP}:{port}/api_control/start_job/error_recover?job_num=1')
    #         API_method.app_start_job('send_error_recover',config_number = config_number)
    #         return {'result':1, "outString":"post successful"}
    #     except Exception as e:
    #         log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
    #         logging.shutdown()
    #         return {'result':0, "outString":str(e)}
    
    ######################################################################################
    
    
    # def job_betradar():
    #     # engine_list = btd.create_connect(config_number = config_number)
    #     global log_handler,logging
    #     log_handler.removeHandler(log_handler.handlers[1])
    #     log_handler,logging = log_config.create_logger(log_file_name)
    #     try:
    #         config_info = API_method.get_config_info(config_number = config_number)
        
    #         # today_Start =pd.to_datetime(datetime.datetime.now())
    #         # IP = API_method.get_IP()
    #         API_method.app_start_job('betradar_to_database',config_number = config_number)
    #         # port = config_info['job_api_port']
    #         # r = requests.post(f'http://{IP}:{port}/api_control/start_job/betradar_to_database?job_num=1')
    #         return {'result':1, "outString":"post successful"}
    #     except Exception as e:
    #         log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
    #         logging.shutdown()
    #         return {'result':0, "outString":str(e)}
        
    ######################################################################################
    def job_check_exist():
        global log_handler,logging
        log_handler.removeHandler(log_handler.handlers[1])
        log_handler,logging = log_config.create_logger(log_file_name)
        try:
            config_info = API_method.get_config_info(config_number = config_number)
            IP = API_method.get_IP()
            port = config_info['job_api_port']
            # r = requests.post(f'http://{IP}:{port}/api_control/check_job')
            API_method.app_check_job_exist(config_number)
            return {'result':1, "outString":"post successful"}
        except Exception as e:
            log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
            logging.shutdown()
            return {'result':0, "outString":str(e)}
    ######################################################################################
    
    # def job_
    
except:
    try:
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()  
        # return {'result':0, "outString":str(e)}
    except:
        
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()
        # return {'result':0, "outString":str(e)}

if __name__ == "__main__":
    
    ## BlockingScheduler 務必要加，若不加程式運行時間較長的話，會有延時過久的錯誤
    scheduler = BlockingScheduler( job_defaults = {'misfire_grace_time':60*60} )
    
    ## add_job 添加排程執行時間點
    # scheduler.add_job(job_loss,"interval",minutes = 5,misfire_grace_time=60*60)
    
    scheduler.add_job(job_check_exist,"interval",minutes = 30,misfire_grace_time=60*60)
    
    # scheduler.add_job(job_betradar,"cron",day_of_week = "mon-sun", hour = 1, minute = 0,misfire_grace_time=60*60)

    
    ## 停止機制
    try:
        
        scheduler.start()
        
    except(KeyboardInterrupt, SystemExit):
        
        scheduler.shutdown(wait = False)
        
        print('Exit The Job!')
