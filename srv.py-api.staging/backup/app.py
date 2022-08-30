# -*- coding: utf-8 -*-
"""
Created on Fri Mar 25 17:32:58 2022

@author: JackeyChen陳威宇
"""

from flask import Flask
from flask_restx import Resource,Api,reqparse
import pandas as pd
import requests
import json
import xmltodict
import os
import sys
import datetime as dt
import method.betradar_data as btd
from sqlalchemy.sql import text as sa_text
import socket
import method.betradar_data as btd
import method.API_method as API_method
import getpass
import warnings
import logging

FORMAT = """%(message)s 
%(asctime)s %(levelname)s """

logging.basicConfig(level=logging.DEBUG, filename=f'./log/app.log', filemode='a',format=FORMAT)

warnings.filterwarnings('ignore')

try:
    # import configparser
    # config_number = '2'
    config_number = sys.argv[1]
    
    engine_list = btd.create_connect(config_number = config_number)
    
    config_info = API_method.get_config_info(config_number = config_number)
    
    env_name = config_info['env_name']
    
    port_number = config_info['job_API_port']
    
    app = Flask(__name__)
    
    api = Api(app, version = '0.0.1', title = env_name +' python API ', dec = '/api/doc')
    # api.add_namespace('Loss_Data')
    # ns = api.namespace('Lost_Data', description='get sr:match or sr:season data by betradar API ( /sports/{language}/sport_events)/{urn_type}:{id}/fixture.xml )')
    ns_lost = api.namespace('Lost_Data')
    
    ns_control = api.namespace('api_control')
    
    job_name = sys.argv[0]
    
    control_info = btd.get_control_info(job_name,config_number = config_number)
    control_IP = control_info['IP'][0]
    control_creator = control_info['Creator'][0]
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "app.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    api_lock_number = 0
    
    #####################################################################################################################
    
    start_job_parser = reqparse.RequestParser()
    # start_job_parser.add_argument('job_name', type=str, required=True)
    start_job_parser.add_argument('job_num', type=int, required=True)
    @ns_control.expect(start_job_parser)
    
    @ns_control.route('/start_job/<string:job_name>')
    @ns_control.param('job_name','程式名稱')
    @ns_control.param('job_num','執行數量')
    
    class start_job(Resource):
        
        def post(self,job_name):
            "執行 job"
            job_num = start_job_parser.parse_args()["job_num"]
            if api_lock_number == 0:
                for i in range(job_num):
                    API_method.app_start_job(job_name, config_number)
            else:
                return "API Lock"
    
    #####################################################################################################################
    @ns_control.route('/stop_job/<string:PID>')
    @ns_control.param('PID','Job PID')
    
    class stop_job(Resource):
        def delete(self,PID):
            "根據pid 中斷程式"
            # PID = '20992'
            if api_lock_number == 0:
                API_method.app_stop_job(PID, config_number)
            else:
                return "API Lock"
    
    #####################################################################################################################
    @ns_lost.route('/restore_data/<string:GameID>')
    @ns_lost.param('GameID','GameID')
    
    class restore_data(Resource):
        def post(self,GameID):
            "根據GameID 重新匯入資料"
            if api_lock_number == 0:
                
                btd.insert_betradar_data(GameID,config_number)
            
                return f"{GameID} post successfully"
            else:
                return "API Lock"
            
    #####################################################################################################################
    @ns_control.route('/check_job')
    # @ns_lost.param('PID','job PID')
    class check_job(Resource):
        def post(self):
            if api_lock_number == 0:
                
                API_method.app_check_job_exist(config_number=config_number)
            else:
                return "API Lock"
    #####################################################################################################################
    @ns_control.route('/stop_all_job')
    # @ns_lost.param('PID','job PID')
    class stop_all_job(Resource):
        def delete(self):
            if api_lock_number == 0:
                
                API_method.app_stop_all_job(config_number=config_number)
            else:
                return "API Lock"
    
    #####################################################################################################################
    @ns_lost.route('/translate_team/<string:TeamID>')
    @ns_lost.param('TeamID','TeamID')
    
    class restore_data(Resource):
        def post(self,TeamID):
            "根據TeamID 重新匯入資料"
            if api_lock_number == 0:
                
                r = btd.update_team_name(TeamID,config_number)
            
                # return f"{GameID} post successfully"
            else:
                return "API Lock"
    # @ns_control.route('/sync_code')
    # # @ns_control.param('source_path','目標位置，請使用反斜線')
    # class sync_code(Resource):
    #     def get(self):
    #         "跟manager機同步程式"
    #         lock_number = 1
    #         global api_lock_number
    #         api_lock_number = int(lock_number)
            
    #         engine_list = btd.create_connect(config_number = config_number)
    #         engine = engine_list[0]
    #         local_ip = API_method.get_IP()
    #         local_ip = '192.168.254.133'
    #         user_info = API_method.get_user_info(local_ip, engine)
    #         manager_info = API_method.get_position_info(engine = engine_list[0], position = 'manager')
    #         source_path = manager_info['Path'][0]
    #         local_path = user_info['Path'][0]
    #         API_method.ssh_scp_files(ssh_host = manager_info['IP'][0],
    #                                   ssh_user = manager_info['UserName'][0],
    #                                   ssh_password = API_method.decode_password(manager_info['Password'][0]),
    #                                   source_path = source_path,
    #                                   local_path = local_path)
            
            
            
            # IP = '192.168.50.48'
            # PID = '16340'
            # hostname = socket.gethostname()
            # local_ip = socket.gethostbyname(hostname)
            
            
        
    
    # @ns_control.route('/lock_api/<string:lock_number>')
    # # @ns_control.param('IP','server IP')
    # @ns_control.param('lock_number','lock_number')
    
    # class lock_api(Resource):
    #     def post(self,lock_number):
    #         global api_lock_number
    #         api_lock_number = int(lock_number)
            
    #         return f"{api_lock_number}"
        
    # @ns_control.route('/test_lock')
    
    # class test_lock(Resource):
    #     def post(self):
    #         global api_lock_number
    #         if api_lock_number == 0:
                
    #             test_string = "no lock"
            
    #         elif api_lock_number == 1:
    #             test_string = "Is lock"
    #         else:
    #             test_string = "error"
            
    #         return f"{test_string}"
    
    
    
    
        
    # if __name__ == '__main__':
    IP = API_method.get_IP()
    app.run(host = IP,port = port_number,debug = False,use_reloader=False)
        # app.run(host = "0.0.0.0",port = 5000,debug = True)

except:
    try:
        logging.exception(f"""#####################{env_name} Catch an exception.#####################""")
    except:
        logging.exception(f"""#####################Catch an exception.#####################""")

    

