# -*- coding: utf-8 -*-
"""
Created on Thu Aug 18 11:01:03 2022

@author: JackeyChen陳威宇
"""

import method.single_API_method as API_method
import method.betradar_data as btd
import sys
import pandas as pd
import os
from sqlalchemy.sql import text as sa_text




config_number = sys.argv[1]
service_action = sys.argv[2]
local_ip = API_method.get_IP()



if service_action == 'start':
    API_method.app_start_job(job_name = 'lost_data_redis', config_number = config_number)
    API_method.app_start_job(job_name = 'lost_data_redis', config_number = config_number)
    API_method.app_start_job(job_name = 'lost_data_redis', config_number = config_number)
    API_method.app_start_job(job_name = 'api_schedule', config_number = config_number)
    # os.system(cmd)
elif service_action == 'restart':
    API_method.app_stop_all_job(config_number = config_number)
    API_method.app_start_job(job_name = 'lost_data_redis', config_number = config_number)
    API_method.app_start_job(job_name = 'lost_data_redis', config_number = config_number)
    API_method.app_start_job(job_name = 'lost_data_redis', config_number = config_number)
    API_method.app_start_job(job_name = 'api_schedule', config_number = config_number)

elif service_action == 'stop':
    API_method.app_stop_all_job(config_number = config_number)
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    app_PID_DF =pd.read_sql(f"""select * from betradar_API_info where IP = "{local_ip}" and Job = "app.py"; """,con =engine)
    if app_PID_DF.shape[0] == 0:
        engine.dispose()
        pass
    else:
        app_PID = app_PID_DF['PID'][0]
        
        engine.execute(sa_text(f'''delete from betradar_API_info where IP = "{local_ip}" and PID = {app_PID} ''').execution_options(autocommit=True))
        engine.dispose()
