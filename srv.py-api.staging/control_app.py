# -*- coding: utf-8 -*-
"""
Created on Wed May 25 14:39:49 2022

@author: User
"""
import method.log_config as log_config
log_file_name = 'control_app'
log_handler,logging = log_config.create_logger(log_file_name)

try:
    from flask import Flask,jsonify,request
    from flask_restx import Resource,Api,reqparse
    import pandas as pd
    import requests
    import json
    import xmltodict
    import os
    import sys
    import datetime as dt
    import socket
    from sqlalchemy.sql import text as sa_text
    import time
    import netifaces
    from netifaces import interfaces, ifaddresses, AF_INET
    import requests
    import configparser
    import warnings
    # sys.path.append(os.path.abspath(os.path.join(os.getcwd(), os.path.pardir)))
    import method.betradar_data as btd
    import method.API_method as API_method
    from flask_cors import CORS
    
    
    warnings.filterwarnings('ignore')
    
    # os.path.abspath()
    
    config_number = sys.argv[1]
    # config_number = '2'
    engine_list = btd.create_connect(config_number = config_number)
    
    config_info = API_method.get_config_info(config_number = config_number)
    
    env_name = config_info['env_name']
    
    port_number = config_info['control_API_port']
    
    app = Flask(__name__)
    # app.config["TEMPLATES_AUTO_RELOAD"] = True
    cors = CORS(app, resources={
    r"/*": {
       "origins": "*"
            }
        })
    api = Api(app, version = '0.0.1', title = env_name +' control API ', dec = '/api/doc')
    # api.add_namespace('Loss_Data')
    ns = api.namespace('Lost_Data', description='get sr:match or sr:season data by betradar API ( /sports/{language}/sport_events)/{urn_type}:{id}/fixture.xml )')
    ns_control = api.namespace('api_control')
    user_control = api.namespace('user_control')
    
    # path = './Error.txt'
    # over_id_dict = {}
        
    job_name = sys.argv[0]
    # print(job_name)
    # job_name = 'D:\PythonCode\python-code\betradar_linux\control_app.py'
    control_info = btd.get_control_info(job_name,config_number = config_number)
    # control_info['Job']
    control_IP = control_info['IP'][0]
    # control_Job = control_info['Job'][0]
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "control_app.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    server_IP = control_info['IP'][0]
    btd.dispose_engine(engine_list)
    #####################################################################################################################
    @ns.route('/get_team/<string:ID>')
    @ns.param('ID', 'Example value: sr:competitor:35')
    class get_team(Resource):
        
        def get(self, ID):
            try:
                "獲取隊伍資料，json格式"
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                result = btd.get_team_api(ID)
                return result
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    
    @ns.route('/get_game_match/<string:ID>')
    @ns.param('ID', 'Example value: sr:match:27647244 or sr:season:83914')
    class get_loss_data_json(Resource):
        # @ns.param('ID', 'Default value: sr:match:27647244 or sr:season:83914')
        def get(self,ID):
            "獲取賽事資料，json格式"
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                result = btd.fixture_match_api(ID)
                return result
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
        
    
    #####################################################################################################################
            
    #####################################################################################################################
    @ns_control.route('/get_open_job')
    class get_open_job(Resource):
        
        def get(self):
            "獲取job紀錄"
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                js = API_method.get_open_job(config_number)
                return js
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    
    #####################################################################################################################
    create_user_parser = reqparse.RequestParser()
    
    create_user_parser.add_argument('encode', type=str, required=True, choices=("True", "False"))
    create_user_parser.add_argument('IP',type = str, required=True)
    create_user_parser.add_argument('user_name',type = str, required=True)
    create_user_parser.add_argument('password',type = str, required=True)
    create_user_parser.add_argument('path',type = str, required=True)
    create_user_parser.add_argument('env_name',type = str, required=True)
    create_user_parser.add_argument('system_name',type = str, required=True,choices = ('windows','linux'))
    create_user_parser.add_argument('position',type = str, required=True,choices = ('manager','worker','rest'))
    
    @user_control.route('/create_user')
    @user_control.expect(create_user_parser)
    
    @api.doc(params={'IP':'目標IP','user_name':'使用者名稱','password':'密碼','path':'程式路徑','env_name':"環境名稱",'encode':'是否加密','system_name':'作業系統','position':'cluster角色'})
    
    class create_user(Resource):
        
        def post(self):
            "新增用戶資料"
            # parser.parse_args()["job_name"]
            # IP = '192.168.18.6'
            # user_name = 'User'
            # password = '147258369'
            # path = 'D:\PythonCode\python-code\\betradar_linux'
            # env_name = 'Stock'
            # encode = 'True'
            # system_name = 'windows'
            # position = 'manager'
            # return path
            # API_method.control_api_create_user(IP, user_name, password, path, env_name, encode, system_name, position, config_number)
            # args = parser.parse_args()
        
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                try:
                    IP = create_user_parser.parse_args()["IP"]
                    user_name = create_user_parser.parse_args()["user_name"]
                    password = create_user_parser.parse_args()["password"]
                    path = create_user_parser.parse_args()["path"]
                    env_name = create_user_parser.parse_args()["env_name"]
                    encode = create_user_parser.parse_args()["encode"]
                    system_name = create_user_parser.parse_args()["system_name"]
                    position = create_user_parser.parse_args()["position"]
                    
                except:
                    post_data = request.get_json(silent=True)
                    IP = post_data["IP"]
                    user_name = post_data["user_name"]
                    password = post_data["password"]
                    path = post_data["path"]
                    env_name = post_data["env_name"]
                    encode = post_data["encode"]
                    system_name = post_data["system_name"]
                    position = post_data["position"]                
                
                API_method.control_api_create_user(IP = IP,
                                                    user_name = user_name,
                                                    password = password,
                                                    path = path,
                                                    env_name = env_name,
                                                    encode = encode,
                                                    system_name = system_name,
                                                    position = position,
                                                    config_number = config_number)
                logging.shutdown()
                return {"result":1,"outString":"post successful"}
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    
    #####################################################################################################################
    update_user_parser = reqparse.RequestParser()
    update_user_parser.add_argument('IP', type=str, required=True)
    update_user_parser.add_argument('user_name', type=str, required=True)
    update_user_parser.add_argument('password', type=str, required=True)
    update_user_parser.add_argument('update_column', type=str, required=True, choices=("IP", "UserName","Password",'Path','EnvName','System','Position'))
    update_user_parser.add_argument('update_value',type=str, required=True)
    @user_control.route('/update_user')
    @user_control.expect(update_user_parser)
    @api.doc(params={'IP':'目標IP','user_name':'使用者名稱','password':'密碼','update_column':'更新欄位','update_value':"更新值"})
    class update_user(Resource):
       
        def patch(self):
            "更新用戶資料"
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                try:
                    IP = create_user_parser.parse_args()["IP"]
                    user_name = create_user_parser.parse_args()["user_name"]
                    password = create_user_parser.parse_args()["password"]
                    update_column = create_user_parser.parse_args()["update_column"]
                    update_value = create_user_parser.parse_args()["update_value"]
                    
                except:
                    post_data = request.get_json(silent=True)
                    IP = post_data["IP"]
                    user_name = post_data["user_name"]
                    password = post_data["password"]
                    update_column = post_data["update_column"]
                    update_value = post_data["update_value"]
                
                API_method.control_api_update_user(IP = IP, 
                                                   user_name = user_name,
                                                   password = password, 
                                                   update_column = update_column, 
                                                   update_value = update_value, 
                                                   config_number = config_number)
                return {"result":1,"outString":"update successful"}
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    #####################################################################################################################
    delete_user_parser = reqparse.RequestParser()
    delete_user_parser.add_argument('IP', type=str, required=True)
    delete_user_parser.add_argument('user_name', type=str, required=True)
    delete_user_parser.add_argument('password', type=str, required=True)
    @user_control.route('/delete_user')
    
    @api.doc(params={'IP':'目標IP','user_name':'使用者名稱','password':'密碼'})
    @user_control.expect(delete_user_parser)
    
    class delete_user(Resource):
        def delete(self):
            "刪除用戶"
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                try:
                    IP = create_user_parser.parse_args()["IP"]
                    user_name = create_user_parser.parse_args()["user_name"]
                    password = create_user_parser.parse_args()["password"]             
                except:
                    post_data = request.get_json(silent=True)
                    IP = post_data["IP"]
                    user_name = post_data["user_name"]
                    password = post_data["password"]
                
                API_method.control_api_delete_user(IP = IP,
                                                   user_name = user_name, 
                                                   password = password, 
                                                   config_number = config_number)
                return {"result":1,"outString":"delete successful"}
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    
    #####################################################################################################################
    
    @ns_control.route('/start_api/<string:IP>')
    @api.doc(params={'IP':'目標IP'})
    class open_api(Resource):
        
        def post(self,IP):
            
            "開啟 API"
            # IP = '192.168.254.133'
            # user_name = 'makise'
            
            API_method.control_api_open_api(IP, config_number)
            
    
    #####################################################################################################################
    @ns_control.route('/stop_api/<string:IP>')
    @api.doc(params={'IP':'目標IP'})
    class close_api(Resource):
        
        def delete(self,IP):
            "關閉 API"
            API_method.control_api_close_api(IP, config_number)
    
    #####################################################################################################################
    @ns_control.route('/stop_job/<string:IP>/<string:PID>')
    @api.doc(params={'IP':'server IP','PID':'Job PID'})
    
    class stop_job(Resource):
        def delete(self,IP,PID):
            "根據pid 中斷job"
            # IP = '192.168.18.6'
            # PID = '17708'
            # config_number = '1'
            API_method.control_api_stop_job(IP, PID, config_number)
                
    #####################################################################################################################
    
    
    start_job_parser = reqparse.RequestParser()
    # start_job_parser.add_argument('IP', type=str,required = True)
    # start_job_parser.add_argument('job_name', type=str,required = True)
    start_job_parser.add_argument('job_num', type=str,required = True)
    @ns_control.route('/start_job/<string:IP>/<string:job_name>')
    @api.doc(params={'IP':'目標IP','job_name':'程式名稱','job_num':"執行數量"})
    @ns_control.expect(start_job_parser)
    
    class start_job(Resource):
        
        def post(self,IP,job_name):
            "開啟 Job"
            job_num = start_job_parser.parse_args()["job_num"]
            # return(job_num)
            API_method.control_api_start_job(IP, job_name,job_num, config_number)
            # API_method.control_api_start_job(IP, job_name, config_number)
    
    
    #####################################################################################################################
    auto_start_job_parser = reqparse.RequestParser()
    # parser.add_argument('IP', type=str)
    auto_start_job_parser.add_argument('job_name',required = True, type=str)
    
    @ns_control.route('/auto_start_job/<string:job_name>')
    # @ns.param('job_name')
    
    class auto_start_job(Resource):
        
        def post(self,job_name):
            "自動分配 Job"
            API_method.control_api_auto_start_job(job_name, config_number)
            # API_method.control_api_start_job(IP, job_name, config_number)
            # return parser.parse_args()["job_name"]
    
    
    #####################################################################################################################
    check_job_exist_parser = reqparse.RequestParser()
    # parser.add_argument('IP', type=str)
    # check_job_exist_parser.add_argument('job_name',required = True, type=str)
    
    @ns_control.route('/check_job_exist')
    # @ns.param('job_name')
    
    class check_job_exist(Resource):
        
        def post(self):
            "監聽程式是否存在，不存在則自動從betradar_API_info中刪除"
            API_method.control_api_check_job_exist(config_number)
            # API_method.control_api_start_job(IP, job_name, config_number)
            # return parser.parse_args()["job_name"]
    
    #####################################################################################################################
    # restart_app_parser = reqparse.RequestParser()
    # parser.add_argument('IP', type=str)
    # check_job_exist_parser.add_argument('job_name',required = True, type=str)
    
    @ns_control.route('/cluster_restart_app')
    # @ns.param('job_name')
    
    class cluster_restart_app(Resource):
        
        def post(self):
            "重啟所有worker的api"
            
            API_method.control_api_cluster_restart_app(config_number)
            # API_method.control_api_start_job(IP, job_name, config_number)
            # return parser.parse_args()["job_name"]
    
    #####################################################################################################################
    # restart_app_parser = reqparse.RequestParser()
    # restart_app_parser.add_argument('IP', type=str)
    # check_job_exist_parser.add_argument('job_name',required = True, type=str)
    
    @ns_control.route('/restart_app/<string:IP>')
    @ns_control.param('IP','IP')
    # @ns.param('job_name')
    class restart_app(Resource):
        
        def post(self,IP):
            "重啟指定worker的api"
            API_method.control_api_restart_app(IP,config_number)
    
    
    #####################################################################################################################
    @ns_control.route('/sync_code/<string:IP>')
    @ns_control.param('IP','IP')
    class sync_code(Resource):
        def post(self,IP):
            "同步指定worker的程式"
            # IP = '192.168.254.134'                         
            API_method.control_api_sync_code(IP,config_number)
    
    
    #####################################################################################################################
    @ns_control.route('/cluster_sync_code')
    # @ns_control.param('IP','IP')
    class cluster_sync_code(Resource):
        def post(self):
            "同步所有worker的程式"
            # IP = '192.168.254.134'                         
            API_method.control_api_cluster_sync_code(config_number = config_number)
    
    #####################################################################################################################
    @ns_control.route('/cluster_stop_all_job')
    # @ns_control.param('IP','IP')
    class cluster_stop_all_job(Resource):
        def post(self):
            "中止所有worker的job"
            # IP = '192.168.254.134'                         
            API_method.control_api_cluster_stop_all_job(config_number = config_number)
    
    #####################################################################################################################
    # if __name__ == '__main__':
    #     job_name = sys.argv[0]
    #     control_info = btd.get_control_info(job_name)
    #     IP = control_info['IP'][0]
    app.run(host = server_IP,port = port_number,debug = False,use_reloader=True)

except:
    try:
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()    
    except:
        log_handler.exception(f"""#####################Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()
