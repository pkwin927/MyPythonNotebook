# -*- coding: utf-8 -*-
"""
Created on Fri Mar 25 17:32:58 2022

@author: JackeyChen陳威宇
"""
import method.log_config as log_config
log_file_name = 'app'
log_handler,logging = log_config.create_logger(log_file_name)

try:
    from flask import Flask,request
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
    import method.single_API_method as API_method
    import getpass
    import warnings
    from flask_cors import CORS
    warnings.filterwarnings('ignore')


    # import configparser
    # config_number = '2'
    config_number = sys.argv[1]
    
    engine_list = btd.create_connect(config_number = config_number)
    
    config_info = API_method.get_config_info(config_number = config_number)
    
    env_name = config_info['env_name']
    
    port_number = config_info['job_API_port']
    
    app = Flask(__name__)
    cors = CORS(app, resources={
    r"/*": {
       "origins": "*"
            }
        })
    api = Api(app, version = '0.0.1', title = env_name +' python API ', dec = '/api/doc')
    # api.add_namespace('Loss_Data')
    # ns = api.namespace('Lost_Data', description='get sr:match or sr:season data by betradar API ( /sports/{language}/sport_events)/{urn_type}:{id}/fixture.xml )')
    ns_lost = api.namespace('Lost_Data')
    
    ns_control = api.namespace('api_control')
    user_control = api.namespace('user_control')
    job_name = sys.argv[0]
    
    control_info = btd.get_control_info(job_name,config_number = config_number)
    control_IP = control_info['IP'][0]
    control_creator = control_info['Creator'][0]
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{control_IP}" and Job = "app.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    api_lock_number = 0
    btd.dispose_engine(engine_list)
    #####################################################################################################################
    
    
    
    #####################################################################################################################
    @ns_lost.route('/get_team/<string:TeamID>')
    @ns_lost.param('TeamID', 'Example value: 35')
    class get_team(Resource):
        
        def get(self, TeamID):

            "獲取隊伍資料，json格式"
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                result = btd.get_team_api(TeamID)
                logging.shutdown()
                return result
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    #####################################################################################################################
    @ns_lost.route('/check_data/<string:GameID>')
    @ns_lost.param('GameID', 'Example value: 35375689')
    class check_data(Resource):
        
        def get(self, GameID):

            "比對database與betradar原始資料，json格式"
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                # GameID = '35375689'
                result = btd.check_betradar_data(GameID,config_number = config_number)
                logging.shutdown()
                # aaa = json.dumps(result)
                return result
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    
    #####################################################################################################################
    @ns_lost.route('/restore_data/<string:GameID>')
    @ns_lost.param('GameID','GameID')
    
    class restore_data(Resource):
        def post(self,GameID):
            "根據GameID 重新匯入資料"
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                btd.insert_betradar_data(GameID,config_number)
                logging.shutdown()
                return {'result':1, "outString":"post success"}
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
            
    #####################################################################################################################
    @ns_control.route('/check_job')
    # @ns_lost.param('PID','job PID')
    class check_job(Resource):
        def post(self):
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                API_method.app_check_job_exist(config_number=config_number)
                logging.shutdown()
                return {'result':1, "outString":"post success"}
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    
    
    #####################################################################################################################
    translate_team_parser = reqparse.RequestParser()

    translate_team_parser.add_argument('team_name', type=str, required=True)
    translate_team_parser.add_argument('translate_team_name',type = str, required=True)
    @ns_lost.expect(translate_team_parser)

    @ns_lost.route('/translate_team')
    @ns_lost.param('team_name','team_name')
    @ns_lost.param('translate_team_name','translate_team_name')

    class translate_team(Resource):
        def post(self):
            """根據TeamName 翻譯資料
            json格式:
            {
                "team_name":aaa, 
                "translate_team_name":bbb
            }"""
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                try:
                    
                    team_name = translate_team_parser.parse_args()['team_name']
                    translate_team_name = translate_team_parser.parse_args()['translate_team_name']
                    
                except:
                    post_data = request.get_json(silent=True)
                    team_name = post_data['team_name']
                    translate_team_name = post_data['translate_team_name']
                    
                API_method.app_translate_team(config_number, team_name, translate_team_name)                   
                logging.shutdown()
                return {'result':1, "outString":"post success"}

            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    #####################################################################################################################
    translate_league_parser = reqparse.RequestParser()
    
    translate_league_parser.add_argument('league_name', type=str, required=True)
    translate_league_parser.add_argument('translate_league_name',type = str, required=True)
    @ns_lost.expect(translate_league_parser)
    
    @ns_lost.route('/translate_league')
    @ns_lost.param('league_name','league_name')
    @ns_lost.param('translate_league_name','translate_league_name')
    
    class translate_league(Resource):
        def post(self):
            """根據leagueName 翻譯資料
            json格式:
            {
                "league_name":aaa, 
                "translate_league_name":bbb
            }"""
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                try:
                    
                    league_name = translate_league_parser.parse_args()['league_name']
                    translate_league_name = translate_league_parser.parse_args()['translate_league_name']
                    
                except:
                    post_data = request.get_json(silent=True)
                    league_name = post_data['league_name']
                    translate_league_name = post_data['translate_league_name']
                    
                API_method.app_translate_league(config_number, league_name, translate_league_name)                   
                logging.shutdown()
                return {'result':1, "outString":"post success"}
    
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    #####################################################################################################################

    
    @ns_control.route('/get_open_job')
    class get_open_job(Resource):
        
        def get(self):
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                js = API_method.get_open_job(config_number)
                logging.shutdown()
                return js
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
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
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                job_num = start_job_parser.parse_args()["job_num"]
                for i in range(job_num):
                    API_method.app_start_job(job_name, config_number)
                logging.shutdown()
                return {'result':1, "outString":"post success"}
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    
    #####################################################################################################################
    
    @ns_control.route('/stop_job/<string:PID>')
    @ns_control.param('PID','Job PID')
    
    class stop_job(Resource):
        def delete(self,PID):
            "根據pid 中斷程式"
            # PID = '20992'
            try:
                global log_handler,logging
                log_handler.removeHandler(log_handler.handlers[1])
                log_handler,logging = log_config.create_logger(log_file_name)
                API_method.app_stop_job(PID, config_number)
                logging.shutdown()
                return {'result':1, "outString":"delete success"}
            except Exception as e:
                log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
                logging.shutdown()
                return {'result':0, "outString":str(e)}
    
    
    
    #####################################################################################################################
    # if __name__ == '__main__':
    IP = API_method.get_IP()
    app.run(host = IP,port = port_number,debug = False,use_reloader=False)
        # app.run(host = "0.0.0.0",port = 5000,debug = True)

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
        # return {'result':0, "outString":str(e)}

