# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 15:53:00 2022

@author: JackeyChen陳威宇
"""


import requests
import pandas as pd
import json
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
import xmltodict
import time
import datetime as dt
import numpy as np
import warnings
import redis
from sqlalchemy.sql import text as sa_text
import socket
import sys
import os
import netifaces
from netifaces import interfaces, ifaddresses, AF_INET
import configparser
import method.single_API_method as API_method
import difflib
import getpass

warnings.filterwarnings('ignore')
# config = configparser.ConfigParser()

# config.read('config.ini')

# database_IP = config['environment']['database_IP']
# database_port = config['environment']['database_port']
# database_user = config['environment']['database_user']
# database_password = config['environment']['database_password']
# redis_IP = config['environment']['redis_IP']
# redis_port = config['environment']['redis_port']
# redis_password = config['environment']['redis_password']
# pd.set_option('display.max_rows',None)
# pd.set_option('display.max_columns',20)
# pd.set_option('display.max_colwidth',None)
# connect_info_betradar = 'mysql+pymysql://jackey:1q2w3e4r@192.168.1.239:3306/betradardb'

# engine_betradar = create_engine(connect_info_betradar,encoding = 'utf8')

# connect_info_main = 'mysql+pymysql://jackey:1q2w3e4r@192.168.1.239:3306/maindb'
# engine_main = create_engine(connect_info_main,encoding = 'utf8')

# series = No_data_DF_matchID
def get_fixture_match(series,method = 'sql'):
    
    today = pd.to_datetime(dt.datetime.now())
    a_day = dt.timedelta(days = 1)
    # creator = config_info["database_user"]
    All_result = pd.DataFrame(columns = ['start_time_confirmed', 'start_time', 'liveodds', 'status',
           'next_live_time', 'id', 'scheduled', 'start_time_tbd',
           'tournament_round', 'season', 'tournament', 'competitors', 'venue',
           'tv_channels', 'extra_info', 'coverage_info', 'product_info',
           'reference_ids', 'scheduled_start_time_changes', 'LanguageCode',
           'CompetitorDict','CreateTime'])
    
    for match_id in series:
        # match_id = str(32585651)
        url = 'https://stgapi.betradar.com/v1/sports/zh/sport_events/sr%3Amatch:'+match_id+'/fixture.xml'

        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
        data_text = response.text
        data_text = data_text.replace("\'",'^')
        data_text = repr(data_text)
        data_text = data_text.replace("\'",'')
        data_text = data_text.replace("\\",'/')

        data_text = data_text.replace('，',' ')
        obj = xmltodict.parse(data_text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        bbb = aaa['fixtures_fixture']['fixture']
        result = pd.DataFrame(columns = ['start_time_confirmed'])
        result.loc[0,'start_time_confirmed'] = aaa['fixtures_fixture']['fixture']['@start_time_confirmed']
        result.loc[0,'start_time'] = aaa['fixtures_fixture']['fixture']['@start_time']
        result['liveodds'] = aaa['fixtures_fixture']['fixture']['@liveodds']
        result['status'] = aaa['fixtures_fixture']['fixture']['@status']
        result['next_live_time'] = aaa['fixtures_fixture']['fixture']['@next_live_time']
        result['id'] = aaa['fixtures_fixture']['fixture']['@id']
        result['scheduled'] = aaa['fixtures_fixture']['fixture']['@scheduled']
        result['start_time_tbd'] = aaa['fixtures_fixture']['fixture']['@start_time_tbd']
        result['tournament_round'] = str(aaa['fixtures_fixture']['fixture']['tournament_round'])
        try:
            result['season'] = str(aaa['fixtures_fixture']['fixture']['season'])
        except:
            result['season'] = ''
            
        result['tournament'] = str(aaa['fixtures_fixture']['fixture']['tournament'])
        result['competitors'] = str(aaa['fixtures_fixture']['fixture']['competitors'])
        
        try:
            result['venue'] = str(aaa['fixtures_fixture']['fixture']['venue'])
        except:
            result['venue'] = ''
            
        result['tv_channels'] = str(aaa['fixtures_fixture']['fixture']['tv_channels'])
        result['extra_info'] = str(aaa['fixtures_fixture']['fixture']['extra_info'])
        
        try:
            result['coverage_info'] = str(aaa['fixtures_fixture']['fixture']['coverage_info'])
        except:
            result['coverage_info'] = ''
            
        result['product_info'] = str(aaa['fixtures_fixture']['fixture']['product_info'])
        
        try:
            result['reference_ids'] = str(aaa['fixtures_fixture']['fixture']['reference_ids'])
        except:
            result['reference_ids'] = ''
        
        try:
            result['scheduled_start_time_changes'] = str(aaa['fixtures_fixture']['fixture']['scheduled_start_time_changes'])
        except:
            result['scheduled_start_time_changes'] = ''
        result['LanguageCode'] = 'zh-cn'
        competitor_id_temp = []
        competitor_name_temp = []
        for competitor_list_num in range(len(aaa['fixtures_fixture']['fixture']['competitors']['competitor'])):
            competitor_id_temp.append(aaa['fixtures_fixture']['fixture']['competitors']['competitor'][competitor_list_num]['@id'])
            competitor_name_temp.append(aaa['fixtures_fixture']['fixture']['competitors']['competitor'][competitor_list_num]['@name'])
        competitor_dict = dict(zip(competitor_id_temp,competitor_name_temp))
        result['CompetitorDict'] = str(competitor_dict)

        for i in result.columns:
            result[i] = result[i].str.replace("'",'"')
            result[i] = result[i].str.replace("^","'",regex=False)

        All_result = pd.concat([All_result,result],axis = 0)
    #     result.to_sql(name = 'sport_events_fixture_sr:match',con = engine,if_exists = 'append',index = False)

        # # ##############################################################################################################################

        url = 'https://stgapi.betradar.com/v1/sports/en/sport_events/sr%3Amatch:'+match_id+'/fixture.xml'

        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
        data_text = response.text
        data_text = data_text.replace("\'",'^')
        data_text = repr(data_text)
        data_text = data_text.replace("\'",'')
        data_text = data_text.replace("\\",'/')

        data_text = data_text.replace('，',' ')
        obj = xmltodict.parse(data_text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        bbb = aaa['fixtures_fixture']['fixture']
        result = pd.DataFrame(columns = ['start_time_confirmed'])
        result.loc[0,'start_time_confirmed'] = aaa['fixtures_fixture']['fixture']['@start_time_confirmed']
        result.loc[0,'start_time'] = aaa['fixtures_fixture']['fixture']['@start_time']
        result['liveodds'] = aaa['fixtures_fixture']['fixture']['@liveodds']
        result['status'] = aaa['fixtures_fixture']['fixture']['@status']
        result['next_live_time'] = aaa['fixtures_fixture']['fixture']['@next_live_time']
        result['id'] = aaa['fixtures_fixture']['fixture']['@id']
        result['scheduled'] = aaa['fixtures_fixture']['fixture']['@scheduled']
        result['start_time_tbd'] = aaa['fixtures_fixture']['fixture']['@start_time_tbd']
        result['tournament_round'] = str(aaa['fixtures_fixture']['fixture']['tournament_round'])
        try:
            result['season'] = str(aaa['fixtures_fixture']['fixture']['season'])
        except:
            result['season'] = ''
        result['tournament'] = str(aaa['fixtures_fixture']['fixture']['tournament'])
        result['competitors'] = str(aaa['fixtures_fixture']['fixture']['competitors'])
        
        try:
            result['venue'] = str(aaa['fixtures_fixture']['fixture']['venue'])
        except:
            result['venue'] = ''
            
        result['tv_channels'] = str(aaa['fixtures_fixture']['fixture']['tv_channels'])
        result['extra_info'] = str(aaa['fixtures_fixture']['fixture']['extra_info'])
        try:
            result['coverage_info'] = str(aaa['fixtures_fixture']['fixture']['coverage_info'])
        except:
            result['coverage_info'] = ''
        
        result['product_info'] = str(aaa['fixtures_fixture']['fixture']['product_info'])
       
        try:
            result['reference_ids'] = str(aaa['fixtures_fixture']['fixture']['reference_ids'])
        except:
            result['reference_ids'] = ''
        try:
            result['scheduled_start_time_changes'] = str(aaa['fixtures_fixture']['fixture']['scheduled_start_time_changes'])
        except:
            result['scheduled_start_time_changes'] = ''
        result['LanguageCode'] = 'en-us'
        competitor_id_temp = []
        competitor_name_temp = []
        for competitor_list_num in range(len(aaa['fixtures_fixture']['fixture']['competitors']['competitor'])):
            competitor_id_temp.append(aaa['fixtures_fixture']['fixture']['competitors']['competitor'][competitor_list_num]['@id'])
            competitor_name_temp.append(aaa['fixtures_fixture']['fixture']['competitors']['competitor'][competitor_list_num]['@name'])

        competitor_dict = dict(zip(competitor_id_temp,competitor_name_temp))
        result['CompetitorDict'] = str(competitor_dict)
        
        if method == 'sql':
            
            for i in result.columns:
                result[i] = result[i].str.replace("'",'"')
                result[i] = result[i].str.replace("^","'",regex=False)
        elif method == 'api':
            for i in result.columns:
                result[i] = result[i].str.replace("^","'",regex=False)
            
        
        All_result = pd.concat([All_result,result],axis = 0)
        
    All_result['CreateTime'] = today
    All_result.reset_index(drop = True, inplace = True)
    return All_result

##########################################################################################################################################

def get_fixture_season(series,method = 'sql'):
    
    today = pd.to_datetime(dt.datetime.now())
    a_day = dt.timedelta(days = 1)
    # creator = database_user
    All_result = pd.DataFrame(columns = ['id','tournament','season','groups','LanguageCode'])
    # series = No_data_DF_seasonID
    for season_id in series:
        # print(season_id)
        # season_id = '90273'
        url = 'https://stgapi.betradar.com/v1/sports/zh/sport_events/sr%3Aseason:'+season_id+'/fixture.xml'
        
        
        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
        data_text = response.text
        data_text = data_text.replace("\'",'^')
        data_text = repr(data_text)
        data_text = data_text.replace("\'",'')
        data_text = data_text.replace("\\",'/')

        data_text = data_text.replace('，',' ')
        obj = xmltodict.parse(data_text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        # if method = 'api':

        SeasonID = aaa['tournament_info']['season']['@id']
        ccc = pd.DataFrame(aaa)
        result = ccc.T
        if 'groups' in result.columns:  
            result = result[['tournament','season','groups']]
        else:
            # pass
            continue
        
        result['id'] = SeasonID
        
        result.reset_index(drop = True,inplace = True)
        for i in result.columns:
            if i in ['scheduled','scheduled_end']:
                pass
            else:
                result[i] = result[i].astype('str')
                
        result['LanguageCode'] = 'zh-cn'
        

        if method == 'sql':
            for i in result.columns:
                result[i] = result[i].str.replace("'",'"')
                result[i] = result[i].str.replace("^","'",regex=False)
        elif method == 'api':
            for i in result.columns:
                result[i] = result[i].str.replace("^","'",regex=False)

        All_result = pd.concat([All_result,result],axis = 0)
    #     result.to_sql(name = 'sport_events_fixture_sr:match',con = engine,if_exists = 'append',index = False)

        # # ##############################################################################################################################

        url = 'https://stgapi.betradar.com/v1/sports/en/sport_events/sr%3Aseason:'+season_id+'/fixture.xml'
        
        
        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
        data_text = response.text
        data_text = data_text.replace("\'",'^')
        data_text = repr(data_text)
        data_text = data_text.replace("\'",'')
        data_text = data_text.replace("\\",'/')

        data_text = data_text.replace('，',' ')
        obj = xmltodict.parse(data_text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        SeasonID = aaa['tournament_info']['season']['@id']
        ccc = pd.DataFrame(aaa)
        result = ccc.T
        if 'groups' in result.columns:  
            result = result[['tournament','season','groups']]
        else:
            # pass
            continue
        
        result['id'] = SeasonID
        
        result.reset_index(drop = True,inplace = True)
        for i in result.columns:
            if i in ['scheduled','scheduled_end']:
                pass
            else:
                result[i] = result[i].astype('str')
                
        result['LanguageCode'] = 'en-us'
        

        for i in result.columns:
            result[i] = result[i].str.replace("'",'"')
            result[i] = result[i].str.replace("^","'",regex=False)

        All_result = pd.concat([All_result,result],axis = 0)
        
    All_result['CreateTime'] = today
    All_result.reset_index(drop = True, inplace = True)
    return All_result

##########################################################################################################################################
# len(json_data['group'])
# def get_from_json(series,col):
#     All_Data = []
#     for i in range(len(series)):
#         # print(i)
#         try:
#             json_data = json.loads(series[i])
#             for k in col:
#                 json_data = json_data[k]
#             All_Data.append(json_data)
#         except:
#             json_data = 'Error'
#             All_Data.append(json_data)
#     return All_Data
# series = targetDF['groups'][0]['group']
# json_data = json.loads(series[i])
# json_data['group']['competitor']
# targetDF['groups'][0]['group']
# col = ['group','competitor']
# series = targetDF['tournament']
# col = ['sport','@id']
# json_data = aaa
def select_json(json_data, select_col,deep = []):

    if isinstance(json_data,(list)):
        json_data_records = []
        for data_number in range(len(json_data)):
            
            try:
                json_data_1 = json_data[data_number]
            except:
                json_data_1 = json_data.copy()
        
            for deep_col in deep:
                json_data_1 = json_data_1[deep_col]
                
            record = {}
            
            for col in select_col:
                if isinstance(col,(list)):
                    column_data = json_data_1.copy()
                    for target_col in col:  
                        column_data = column_data[target_col]
                    record[target_col] = column_data
                else: 
                    column_data = json_data_1[col]
                    record[col] = column_data
            
            json_data_records.append(record)
            
        return json_data_records
    else:
        raise TypeError("data is not a list")

# series = targetDF['groups']
# col = ['group','loop','competitor']
def get_from_json(series,col):
    All_Data = []
    for i in range(len(series)):
        # i = 0
        # str_json = json.dumps(series[i])
        try:
            json_data = json.loads(series[i])
        except:
            str_json = series[i].replace('None','"None"')
            json_data = json.loads(str_json)
        # json_data = series[i]
        col_number = -1
        try:
            for k in range(len(col)):
                # k = 2
                # print(k)
                if k == col_number:
                    continue
                col_name = col[k]
                # json_data['competitor']
                if col_name == 'loop':
                    json_data_temp = []
                    for j in range(len(json_data)):
                        # j = 0
                        col_number = k+1
                        col_name = col[col_number]
                        try:
                            one_json_data = json_data[j][col_name]
                            
                            if isinstance(one_json_data,str):
                                json_data_temp.append(one_json_data)
                            elif isinstance(one_json_data,list):
                                for m in range(len(one_json_data)):
                                    json_data_temp.append(one_json_data[m])
                            
                            elif isinstance(one_json_data,dict):
                                json_data_temp.append(one_json_data)
                            
                            # elif len(one_json_data) > 1:
                            #     for m in range(len(one_json_data)):
                            #         json_data_temp.append(one_json_data[m])
                            else:
                                json_data_temp.append(one_json_data)
                        except:
                            col_number = k+1
                            col_name = col[col_number]
                            json_data_temp = json_data[col_name]
                            break
                        
                    json_data = json_data_temp
                else:
                    json_data = json_data[col_name]
            
            All_Data.append(json_data)
        except:
            json_data = 'Error'
            All_Data.append(json_data)
    return All_Data
##########################################################################################################################################

def get_game_match_data(targetDF,config_number,UTC = 8):
    # engine_main = create_engine(connect_info_main,encoding = 'utf8')
    config_info = API_method.get_config_info(config_number = config_number)
    today = pd.to_datetime(dt.datetime.now())
    
    a_day = dt.timedelta(days = 1)
    UTC_time = dt.timedelta(hours = UTC)
    # tz_uk = dt.timezone(dt.timedelta(hours=8))
    creator = config_info['database_user']

    targetDF['Sport_DataSourceID'] = get_from_json(targetDF['tournament'],['sport','@id'])
    targetDF['League_DataSourceID'] = get_from_json(targetDF['tournament'],['@id'])
    targetDF['Region_DataSourceID'] = get_from_json(targetDF['tournament'],['category','@id'])
    targetDF['SourceTeam_DataSourceID'] = get_from_json(targetDF['competitors'],['competitor',0,'@id'])
    targetDF['TargetTeam_DataSourceID'] = get_from_json(targetDF['competitors'],['competitor',1,'@id'])

    
    # match_DF = match_DF.loc[match_DF['id'] == 'sr:match:27751624']
    Team_List = []
    for i in range(len(targetDF['competitors'])):
        # try:
        json_data = json.loads(targetDF['competitors'][i])
        # except:
            # print('Error')
            # f = open(error_path, 'a')
            # GameID = targetDF.loc[i,'id']
            # f.write(today.strftime('%Y/%m/%d %H:%M:%S') +' Error Data '+str(GameID)+' competitors Error \n')
            # f.close()
            # continue
        GameID = targetDF.loc[i,'id']
        LeagueID = targetDF.loc[i,'League_DataSourceID']
        LanguageCode = targetDF.loc[i,'LanguageCode']
        for k in range(len(json_data['competitor'])):
            qualifier = json_data['competitor'][k]['@qualifier']
            TeamID = json_data['competitor'][k]['@id']
            TeamName = json_data['competitor'][k]['@name']
            DataSourceType = 1
            Team_data = [TeamID,TeamName,LeagueID,LanguageCode, creator, today,DataSourceType,GameID,qualifier]
            Team_List.append(Team_data)
            
    Team_DF = pd.DataFrame(Team_List,columns = ['TeamID','TeamName','LeagueID','LanguageCode','Creator','UpdateTime','DataSourceType','GameID','Type'])
    Team_DF['GameID'] = Team_DF['GameID'].str.rsplit(':',1,expand = True)[1]
    Team_DF['GameID'] = Team_DF['GameID'].astype(int)

    Team_DF['TeamID'] = Team_DF['TeamID'].str.rsplit(':',1,expand = True)[1]
    Team_DF['TeamID'] = Team_DF['TeamID'].astype(int)

    Team_DF['LeagueID'] = Team_DF['LeagueID'].str.rsplit(':',1,expand = True)[1]
    Team_DF['LeagueID'] = Team_DF['LeagueID'].astype(int)

    Team_DF['Type'] = Team_DF['Type'].map({'home':1,'away':2})
    Team_DF['Updator'] = creator

    # Game_Team_DF
    Game_Team_DF = Team_DF.loc[Team_DF['LanguageCode'] == 'zh-cn']
    Game_Team_DF = Game_Team_DF[['GameID','TeamID','Type','Creator','Updator','UpdateTime','DataSourceType']]
    Game_Team_DF.drop_duplicates(['GameID','TeamID','DataSourceType'],inplace = True)
    # Game_Team_DF.to_sql(name = 'game_team_list',con = connect_info_main,if_exists = 'append',index = False)
    # Game_Team_DF.to_sql(name = 'game_team_list',con = connect_info_test,if_exists = 'append',index = False)

    Team_DF.drop_duplicates(['TeamID','LeagueID','LanguageCode','DataSourceType'],inplace = True)
    Team_DF = Team_DF[['TeamID','TeamName','LeagueID','LanguageCode','Creator','Updator','UpdateTime','DataSourceType']]
    # Team_DF.to_sql(name = 'game_team',con = connect_info_main,if_exists = 'append',index = False)
    # Team_DF.to_sql(name = 'game_team',con = connect_info_test,if_exists = 'append',index = False)

    ## game_match

    GameMatch_DF = targetDF[['id','Sport_DataSourceID','Region_DataSourceID','League_DataSourceID','SourceTeam_DataSourceID','TargetTeam_DataSourceID','start_time','LanguageCode','status','liveodds']]
    GameMatch_DF = GameMatch_DF.loc[GameMatch_DF['LanguageCode'] == 'zh-cn']
    GameMatch_DF.drop(['LanguageCode'], axis = 1, inplace = True)
    GameMatch_DF['DataSourceEventID'] = GameMatch_DF['id']
    GameMatch_DF['id'] = GameMatch_DF['id'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['id'] = GameMatch_DF['id'].astype(int)

    GameMatch_DF['Sport_DataSourceID'] = GameMatch_DF['Sport_DataSourceID'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['Sport_DataSourceID'] = GameMatch_DF['Sport_DataSourceID'].astype(int)

    GameMatch_DF['Region_DataSourceID'] = GameMatch_DF['Region_DataSourceID'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['Region_DataSourceID'] = GameMatch_DF['Region_DataSourceID'].astype(int)

    GameMatch_DF['League_DataSourceID'] = GameMatch_DF['League_DataSourceID'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['League_DataSourceID'] = GameMatch_DF['League_DataSourceID'].astype(int)

    GameMatch_DF['SourceTeam_DataSourceID'] = GameMatch_DF['SourceTeam_DataSourceID'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['SourceTeam_DataSourceID'] = GameMatch_DF['SourceTeam_DataSourceID'].astype(int)

    GameMatch_DF['TargetTeam_DataSourceID'] = GameMatch_DF['TargetTeam_DataSourceID'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['TargetTeam_DataSourceID'] = GameMatch_DF['TargetTeam_DataSourceID'].astype(int)

    GameMatch_DF['Creator'] = creator
    GameMatch_DF['Updator'] = creator
    GameMatch_DF['start_time'] = pd.to_datetime(GameMatch_DF['start_time'],utc=True)
    GameMatch_DF['end_time'] = GameMatch_DF['start_time'] + 7*a_day
    GameMatch_DF['UpdateTime'] = today
    # GameMatch_DF['HasStream'] = 0
    GameMatch_DF['DataSourceType'] = 1
    GameMatch_DF['liveodds'] = GameMatch_DF['liveodds'].map({'buyable':1,'bookable':2, 'booked':3, 'not_available':4})
    GameMatch_DF['status'] = GameMatch_DF['status'].map({'not_started':0,'match_about_to_start':11, 
                                                         'postponed':8, 'delayed':6, 'cancelled':5, 
                                                         'interrupted':7, 'suspended':2,
                                                         'abandoned':9, 'ended':3,
                                                         'unknown':10, 
                                                         'closed':4,
                                                         'live':1})
    
    GameMatch_DF.loc[GameMatch_DF['liveodds'].isnull(),'liveodds'] = 99
    GameMatch_DF.loc[GameMatch_DF['status'].isnull(),'status'] = 99
    GameMatch_DF['SeasonType'] = 1
    
    GameMatch_DF.rename(columns = {'id':'GameID', 'Sport_DataSourceID':'KindID','Region_DataSourceID':'RegionID', 
                                   'League_DataSourceID':'LeagueID','SourceTeam_DataSourceID':'SourceTeamID',
                                   'TargetTeam_DataSourceID':'TargetTeamID','start_time':'GameStartTimeUtc0','end_time':'GameEndTimeUtc0',
                                   'status':'Status','liveodds':'LiveOdds'},  inplace = True)
    
    GameMatch_DF.drop_duplicates(inplace = True)
    # GameMatch_DF['GameStartTimeUtc0'] = pd.to_datetime(GameMatch_DF['GameStartTimeUtc0'],utc = True)
    # GameMatch_DF['GameEndTimeUtc0'] = pd.to_datetime(GameMatch_DF['GameEndTimeUtc0'],utc = True)
    GameMatch_DF['GameStartTime'] = GameMatch_DF['GameStartTimeUtc0'] + UTC_time
    GameMatch_DF['GameEndTime'] = GameMatch_DF['GameEndTimeUtc0'] + UTC_time
    
    # GameMatch_DF.to_sql(name = 'game_match',con = connect_info_main,if_exists = 'append',index = False)
    # GameMatch_DF.to_sql(name = 'game_match',con = connect_info_test,if_exists = 'append',index = False)
    return Game_Team_DF,Team_DF,GameMatch_DF
##########################################################################################################################################
def game_match_CheckExist(engine_main,Game_Team_DF,Team_DF,GameMatch_DF):
    game_team_DF = pd.read_sql('select TeamID as CheckExist,TeamID,LeagueID, LanguageCode,DataSourceType  from game_team',con = engine_main)
    game_team_list_DF = pd.read_sql('select GameID as CheckExist,GameID,TeamID,DataSourceType from game_team_list',con = engine_main)
    game_match_DF = pd.read_sql('select GameID as CheckExist,GameID,DataSourceType from game_match',con = engine_main)

    Game_Team_DF = pd.merge(Game_Team_DF,game_team_list_DF, on = ['GameID','TeamID','DataSourceType'], how = 'left')
    Game_Team_DF = Game_Team_DF.loc[Game_Team_DF['CheckExist'].isnull()]
    Game_Team_DF.drop(['CheckExist'], axis = 1, inplace = True)
    
    Team_DF = pd.merge(Team_DF,game_team_DF, on = ['TeamID','LeagueID','LanguageCode','DataSourceType'], how = 'left')
    Team_DF = Team_DF.loc[Team_DF['CheckExist'].isnull()]
    Team_DF.drop(['CheckExist'], axis = 1, inplace = True)
    
    GameMatch_DF = pd.merge(GameMatch_DF,game_match_DF, on = ['GameID','DataSourceType'], how = 'left')
    GameMatch_DF = GameMatch_DF.loc[GameMatch_DF['CheckExist'].isnull()]
    GameMatch_DF.drop(['CheckExist'], axis = 1, inplace = True)
    
    return Game_Team_DF,Team_DF,GameMatch_DF
##########################################################################################################################################
    # season_DF = season_data
def get_game_season_data(targetDF,config_number,UTC = 8):
    config_info = API_method.get_config_info(config_number = config_number)
    today = pd.to_datetime(dt.datetime.now())
    a_day = dt.timedelta(days = 1)
    UTC_time = dt.timedelta(hours = UTC)
    creator = config_info["database_user"]
    
    targetDF['Sport_DataSourceID'] = get_from_json(targetDF['tournament'],['sport','@id'])
    targetDF['League_DataSourceID'] = get_from_json(targetDF['tournament'],['@id'])
    targetDF['Region_DataSourceID'] = get_from_json(targetDF['tournament'],['category','@id'])
    targetDF['competitors'] = get_from_json(targetDF['groups'],['group','loop','competitor'])
    targetDF['start_time'] = get_from_json(targetDF['season'],['@start_date'])
    targetDF['end_time'] = get_from_json(targetDF['season'],['@end_date'])
    # game_team_DF = pd.read_sql('select TeamID as CheckExist,TeamID,LeagueID, LanguageCode,DataSourceType  from game_team',con = engine_main)
    # game_team_list_DF = pd.read_sql('select GameID as CheckExist,GameID,TeamID from game_team_list',con = engine_main)
    
    Team_List = []
    
    for i in range(len(targetDF['competitors'])):
        # i = 0
        GameID = targetDF.loc[i,'id']
        LeagueID = targetDF.loc[i,'League_DataSourceID']
        LanguageCode = targetDF.loc[i,'LanguageCode']
        for j in range(len(targetDF['competitors'][i])):

            TeamID = targetDF['competitors'][i][j]['@id']
            TeamName = targetDF['competitors'][i][j]['@name']
            DataSourceType = 1
            Team_data = [TeamID,TeamName,LeagueID,LanguageCode, creator, today,DataSourceType,GameID]
            Team_List.append(Team_data)

        
    Team_DF = pd.DataFrame(Team_List,columns = ['TeamID','TeamName','LeagueID','LanguageCode','Creator','UpdateTime','DataSourceType','GameID'])
    Team_DF['GameID'] = Team_DF['GameID'].str.rsplit(':',1,expand = True)[1]
    Team_DF['GameID'] = Team_DF['GameID'].astype(int)

    Team_DF['TeamID'] = Team_DF['TeamID'].str.rsplit(':',1,expand = True)[1]
    Team_DF['TeamID'] = Team_DF['TeamID'].astype(int)

    Team_DF['LeagueID'] = Team_DF['LeagueID'].str.rsplit(':',1,expand = True)[1]
    Team_DF['LeagueID'] = Team_DF['LeagueID'].astype(int)
    Team_DF['Type'] = 0
    Team_DF['Updator'] = creator
    
    Game_Team_DF = Team_DF.loc[Team_DF['LanguageCode'] == 'zh-cn']
    Game_Team_DF = Game_Team_DF[['GameID','TeamID','Type','Creator','Updator','UpdateTime','DataSourceType']]
    Game_Team_DF.drop_duplicates(['GameID','TeamID','DataSourceType'],inplace = True)
    
    Team_DF.drop_duplicates(['TeamID','LeagueID','LanguageCode','DataSourceType'],inplace = True)
    Team_DF = Team_DF[['TeamID','TeamName','LeagueID','LanguageCode','Creator','Updator','UpdateTime','DataSourceType']]
    
    GameMatch_DF = targetDF[['id','Sport_DataSourceID','Region_DataSourceID','League_DataSourceID','start_time','LanguageCode','end_time']]
    GameMatch_DF = GameMatch_DF.loc[GameMatch_DF['LanguageCode'] == 'zh-cn']
    GameMatch_DF.drop(['LanguageCode'], axis = 1, inplace = True)
    GameMatch_DF['DataSourceEventID'] = GameMatch_DF['id']
    GameMatch_DF['id'] = GameMatch_DF['id'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['id'] = GameMatch_DF['id'].astype(int)

    GameMatch_DF['Sport_DataSourceID'] = GameMatch_DF['Sport_DataSourceID'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['Sport_DataSourceID'] = GameMatch_DF['Sport_DataSourceID'].astype(int)

    GameMatch_DF['Region_DataSourceID'] = GameMatch_DF['Region_DataSourceID'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['Region_DataSourceID'] = GameMatch_DF['Region_DataSourceID'].astype(int)

    GameMatch_DF['League_DataSourceID'] = GameMatch_DF['League_DataSourceID'].str.rsplit(':',1,expand = True)[1]
    GameMatch_DF['League_DataSourceID'] = GameMatch_DF['League_DataSourceID'].astype(int)
    
    GameMatch_DF['Creator'] = creator
    GameMatch_DF['Updator'] = creator
    GameMatch_DF['start_time'] = pd.to_datetime(GameMatch_DF['start_time'])
    GameMatch_DF['end_time'] = pd.to_datetime(GameMatch_DF['end_time'])
    GameMatch_DF['UpdateTime'] = today
    # GameMatch_DF['HasStream'] = 0
    GameMatch_DF['DataSourceType'] = 1
    # GameMatch_DF['liveodds'] = GameMatch_DF['liveodds'].map({'buyable':1,'bookable':2, 'booked':3, 'not_available':4})
    # GameMatch_DF['status'] = GameMatch_DF['status'].map({'not_started':1,'match_about_to_start':1, 
    #                                                      'postponed':2, 'delayed':2, 'cancelled':2, 
    #                                                      'interrupted':2, 'suspended':2,
    #                                                      'abandoned':2, 'ended':2,
    #                                                      'aet':2, 'match_after_penalties':2,
    #                                                      'closed':2,'live':2})
    
    
    GameMatch_DF['liveodds'] = 99
    GameMatch_DF['status'] = 99
    GameMatch_DF['SeasonType'] = 2
    # GameMatch_DF.loc[GameMatch_DF['liveodds'].isnull(),'liveodds'] = 5
    # GameMatch_DF.loc[GameMatch_DF['status'].isnull(),'status'] = 2
    
    
    GameMatch_DF.rename(columns = {'id':'GameID', 'Sport_DataSourceID':'KindID','Region_DataSourceID':'RegionID', 
                                   'League_DataSourceID':'LeagueID','SourceTeam_DataSourceID':'SourceTeamID',
                                   'TargetTeam_DataSourceID':'TargetTeamID','start_time':'GameStartTimeUtc0','end_time':'GameEndTimeUtc0',
                                    'liveodds':'LiveOdds','status':'Status'
                                   },  inplace = True)
    
    GameMatch_DF.drop_duplicates(['GameID','DataSourceType'],inplace = True)
    
    GameMatch_DF['GameStartTime'] = GameMatch_DF['GameStartTimeUtc0'] + UTC_time
    GameMatch_DF['GameEndTime'] = GameMatch_DF['GameEndTimeUtc0'] + UTC_time
    
    return Game_Team_DF,Team_DF,GameMatch_DF


##########################################################################################################################################

def get_game_league_data(targetDF,engine_main,config_number):
    config_info = API_method.get_config_info(config_number = config_number)
    today = pd.to_datetime(dt.datetime.now())
    a_day = dt.timedelta(days = 1)
    creator = config_info["database_user"]
    game_league_DF = pd.read_sql('select LeagueID,DataSourceType,LanguageCode,Creator,LeagueID as CheckExist from game_league',con = engine_main)

    targetDF['KindID'] = targetDF['sport'].str.split(',',1,expand = True)[0].str.rsplit(':',1,expand = True)[1].str.replace("'","")
    targetDF['RegionID'] = targetDF['category'].str.split(',',1,expand = True)[0].str.rsplit(':',1,expand = True)[1].str.replace("'","")

    targetDF['KindID'] = targetDF['KindID'].astype(int)
    targetDF['RegionID'] = targetDF['RegionID'].astype(int)

    targetDF = targetDF.loc[~targetDF['id'].str.contains('sr:simple_tournament')]
    targetDF['id'] = targetDF['id'].str.rsplit(':',1,expand = True)[1]
    targetDF['id'] = targetDF['id'].astype(int)
    targetDF.rename(columns = {'id':'LeagueID','name':'LeagueName'}, inplace = True)
    # targetDF['Creator'] = creator
    targetDF['Updator'] = creator
    targetDF['UpdateTime'] = today
    # targetDF['Level'] = 2
    # targetDF['IsTop'] = 0
    # targetDF['OrderBy'] = 0
    targetDF['DataSourceType'] = 1

    targetDF = pd.merge(targetDF,game_league_DF,on = ['LeagueID','DataSourceType','LanguageCode'], how = 'left')
    targetDF.loc[targetDF['CheckExist'].isnull(),'Creator'] = creator
    
    targetDF.drop(['CheckExist'], axis = 1, inplace = True)
    targetDF = targetDF[['LeagueID','LeagueName','KindID','RegionID','LanguageCode','Creator','Updator','UpdateTime','DataSourceType']]
    engine_main.dispose()
    return targetDF

def get_game_region_data(targetDF,engine_main,config_number):
    # engine_main = create_engine(connect_info_main,encoding = 'utf8')
    config_info = API_method.get_config_info(config_number = config_number)

    today = pd.to_datetime(dt.datetime.now())
    a_day = dt.timedelta(days = 1)
    creator = config_info["database_user"]
    
    game_region_DF = pd.read_sql('select RegionID, DataSourceType,LanguageCode,Creator,RegionID as CheckExist from game_region',con = engine_main)

    targetDF['id'] = targetDF['id'].str.rsplit(':',1,expand = True)[1]
    targetDF['id'] = targetDF['id'].astype(int)
    targetDF.rename(columns = {'id':'RegionID','name':'RegionName'}, inplace = True)

    targetDF['DataSourceType'] = 1
    

    targetDF = pd.merge(targetDF,game_region_DF,on = ['RegionID','DataSourceType','LanguageCode'], how = 'left')
    targetDF.loc[targetDF['CheckExist'].isnull(),'Creator'] = creator
    targetDF.sort_values('RegionID',inplace = True)
    targetDF.drop(['CheckExist'], axis = 1, inplace = True)

    today = pd.to_datetime(dt.datetime.now())
    targetDF['Creator'] = creator
    targetDF['CreateTime'] = today
    # targetDF['Creator'] = creator
    # targetDF['CreateTime'] = today
    targetDF = targetDF[['LanguageCode','RegionID','RegionName','Creator','CreateTime','DataSourceType']]
    engine_main.dispose()
    return targetDF

def get_game_kind_data(targetDF,engine_main,config_number):
    # engine_main = create_engine(connect_info_main,encoding = 'utf8')
    config_info = API_method.get_config_info(config_number = config_number)
    today = pd.to_datetime(dt.datetime.now())
    a_day = dt.timedelta(days = 1)
    creator = config_info["database_user"]
    
    game_kind_DF = pd.read_sql('select KindID,DataSourceType,LanguageCode,Creator,KindID as CheckExist from game_kind',con = engine_main)

    targetDF['id'] = targetDF['id'].str.rsplit(':',1,expand = True)[1]
    targetDF['id'] = targetDF['id'].astype(int)

    targetDF.columns = ['KindID','KindName','LanguageCode']
    # targetDF = targetDF.loc[targetDF['KindID'].isin([1,2,3,5])]
    # targetDF['Creator'] = creator
    today = pd.to_datetime(dt.datetime.now())
    # targetDF['CreateTime'] = today
    # targetDF['IsPageLinkType'] = 0
    # targetDF['OrderBy'] = 1
    targetDF['DataSourceType'] = 1
    targetDF = pd.merge(targetDF,game_kind_DF,on = ['KindID','DataSourceType','LanguageCode'], how = 'left')
    targetDF.loc[targetDF['CheckExist'].isnull(),'Creator'] = creator
    targetDF.drop(['CheckExist'], axis = 1, inplace = True)
    today = pd.to_datetime(dt.datetime.now())
    targetDF['Updator'] = creator
    targetDF['UpdateTime'] = today
    
    engine_main.dispose()
    return targetDF


def fixture_match_api(ID):
    # ID = 'sr:season:26251720'
    # ID = 'sr:match:26251720465456'
    ID_list = ID.rsplit(':',1)
    ID_type = ''
    if ID_list[0] == 'sr:match':
        ID_type = 'sr%3Amatch:'
        
    elif ID_list[0] == 'sr:season':
        ID_type = 'sr%3Aseason:'
        
    else:
        raise ValueError(ID +'incorrect format，just in sr:match or sr:season')
    try:
        url = 'https://stgapi.betradar.com/v1/sports/zh/sport_events/'+ID_type+ID_list[1]+'/fixture.xml'
        
        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
        data_text = response.text
        data_text = data_text.replace("\'",'^')
        data_text = repr(data_text)
        data_text = data_text.replace("\'",'')
        data_text = data_text.replace("\\",'/')
        data_text = data_text.replace('，',' ')
        data_text = data_text.replace("^","'")
        obj = xmltodict.parse(data_text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        result = aaa
        # result = select_json(json_data = [aaa], select_col = ['@start_time','competitors'],deep = ['fixtures_fixture','fixture'])
    
    except Exception as e:
        result = {'Error':repr(e)}
    return result

def get_team_api(TeamID):
    # ID = 'sr:competitor:35'
    try:
        # ID_list = ID.rsplit(':',1)
        url = 'https://stgapi.betradar.com/v1/sports/zh/competitors/sr:competitor:'+TeamID+'/profile.xml'
        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        data_text = response.text
        data_text = data_text.replace("\'",'^')
        data_text = repr(data_text)
        data_text = data_text.replace("\'",'')
        data_text = data_text.replace("\\",'/')
        data_text = data_text.replace('，',' ')
        data_text = data_text.replace("^","'")
        obj = xmltodict.parse(data_text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        result = aaa
    except Exception as e:
        result = {'Error':repr(e)}
    return result
    

def create_connect(config_number):
    # config_number = '1'
    config_info = API_method.get_config_info(config_number = config_number)
    
    database_IP = config_info['database_IP']
    database_port = config_info['database_port']
    database_user = config_info['database_user']
    database_password = config_info['database_password']
    api_database_name = config_info['api_database_name']
    game_database_name = config_info['game_database_name']
    # database_password = "!Q\@W3e4r"
    connect_info_betradar = f"""mysql+pymysql://{database_user}:{database_password}@{database_IP}:{database_port}/{api_database_name}"""
    engine_betradar = create_engine(connect_info_betradar,encoding = 'utf8',connect_args={"program_name":"python"})

    # connect_info_betradar = 'mysql+pymysql://jackey:1q2w3e4r@18.163.118.239:3306/betradardb'
    # engine_betradar = create_engine(connect_info_betradar,encoding = 'utf8')
    
    # connect_info = 'mysql+pymysql://jackey:1q2w3e4r@18.163.118.239:3306/maindb'
    # engine_main = create_engine(connect_info,encoding = 'utf8')

    connect_info =  f"""mysql+pymysql://{database_user}:{database_password}@{database_IP}:{database_port}/{game_database_name}"""
    engine_main = create_engine(connect_info,encoding = 'utf8',connect_args={"program_name":"python"})
    
    return [engine_betradar,engine_main]

def create_redis(config_number):
    config_info = API_method.get_config_info(config_number = config_number)
    redis_IP = config_info['redis_IP']
    redis_port = config_info['redis_port']
    try:
        redis_password = config_info['redis_password']
        redis_pool = redis.ConnectionPool(host=redis_IP, port=int(redis_port),password = redis_password, decode_responses=True)
    except:
        redis_pool = redis.ConnectionPool(host=redis_IP, port=int(redis_port), decode_responses=True)

    redis_conn= redis.StrictRedis(connection_pool=redis_pool)
    
    return redis_conn


def dispose_engine(engine_list):
    for engine in engine_list:
        engine.dispose()


def update_duplicate_to_sql(insert_DF,table_name,engine):

    for i in range(insert_DF.shape[0]):
        # i = 1
        insert_data = insert_DF.iloc[i]
        
        sql_table_column_str =str([i for i in insert_data.index]).replace('[','(').replace(']',')').replace("'","")
        sql_table_values_str = str([i for i in insert_data]).replace('[','(').replace(']',')').replace(", tz='UTC'","").replace("+0000","")
        
        sql_table_update_columns_list = [i+' = '+'VALUES('+i+')' for i in insert_data.index]
        
        sql_commend_str = """INSERT INTO """+table_name+""" """+ sql_table_column_str +""" VALUES """+sql_table_values_str+""" ON DUPLICATE KEY UPDATE """
        
        for sql_table_update_columns_str in sql_table_update_columns_list:

            if sql_table_update_columns_list.index(sql_table_update_columns_str) + 1 == len(sql_table_update_columns_list):
                
                sql_commend_str = sql_commend_str + sql_table_update_columns_str +';'
            else:
            
                sql_commend_str = sql_commend_str + sql_table_update_columns_str +', '
        
        engine.execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    engine.dispose()

def delete_update_to_sql(insert_DF,table_name,engine):
    # for i in range(insert_DF.shape[0]):
    # insert_data = insert_DF.iloc[i]
    game_id = insert_DF['GameID'].unique()[0]
    sql_commend_str = f"DELETE FROM {table_name} WHERE GameID = {game_id};"
    engine.execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    insert_DF.to_sql(name = table_name ,con = engine,if_exists = 'append',index = False)
    


def update_game_league(engine_list,config_number):
    
    engine_betradar = engine_list[0]
    engine_main = engine_list[1]
    
    # engine_betradar.execute(sa_text('''TRUNCATE TABLE tournaments_all''').execution_options(autocommit=True))
    
    All_result = pd.DataFrame(columns = ['id', 'name', 'sport', 'category', 
                                         'current_season','season_coverage_info', 
                                         'exhibition_games', 'scheduled',
                                         'scheduled_end', 'LanguageCode'])
    for language_code in ['zh','en']:
        # language_code = 'zh'
        url = 'https://stgapi.betradar.com/v1/sports/'+language_code+'/tournaments.xml'
        
        headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
        response = requests.get(url,headers = headers)
        # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
        obj = xmltodict.parse(response.text)
        aaa = json.dumps(obj)
        aaa = json.loads(aaa)
        bbb = aaa['tournaments']['tournament']
        result = pd.DataFrame(bbb)
        result_columns = []
        for i in range(len(result.columns)):
            result_columns.append(result.columns[i].replace('@',''))
        result.columns = result_columns
        
        for i in result.columns:
            if i in ['scheduled','scheduled_end']:
                pass
            else:
                result[i] = result[i].astype('str')
        # result = result.head(5)
        if language_code == 'zh':
            result['LanguageCode'] = 'zh-cn'
        elif language_code == 'en':
            result['LanguageCode'] = 'en-us'
        All_result = pd.concat([All_result,result],axis = 0)
        
    # All_result.to_sql(name = 'tournaments_all',con = engine_betradar,if_exists = 'append',index = False)
    
    league_DF = All_result.copy()
    
    league_DF = get_game_league_data(targetDF = league_DF,engine_main = engine_main,config_number = config_number)
    try:
        league_DF.to_sql(name = 'game_league',con = engine_main,if_exists = 'append',index = False)
        dispose_engine(engine_list)
        return league_DF
    except:
        league_DF = pd.DataFrame(columns = ['LeagueID','LeagueName','KindID','RegionID','LanguageCode','Creator', 'CreateTime','DataSourceType'])
        dispose_engine(engine_list)
        return league_DF
    
    

def get_control_info(job_name,config_number):
    config_info = API_method.get_config_info(config_number = config_number)
    # st = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # st.connect(("8.8.8.8", 80))
    # IP = st.getsockname()[0]
    # path = os.getcwd()
    # hostname = socket.gethostname()
    user_name = getpass.getuser()
    # IP = socket.gethostbyname(hostname)
    ifaceName = netifaces.gateways()['default'][2][1]
    IP = [i['addr'] for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr':'No IP addr'}] )][0]
    # job_name = "D:\PythonCode\python-code\betradar_linux\control_app.py"
    # job_name = "control_app.py"
    all_path = job_name.rsplit('\\',1)
    if len(all_path)== 1:
        all_path = all_path[0]
    else:
        all_path = all_path[1]
        
    all_path = all_path.rsplit('/',1)
    if len(all_path)== 1:
        all_path = all_path[0]
    else:
        all_path = all_path[1]
    
    PID = os.getpid()
    CreateTime = pd.to_datetime(dt.datetime.now())
    Creator = user_name
    
    control_info = pd.DataFrame([[IP,all_path,PID,CreateTime,Creator]],columns = ['IP','Job','PID','CreateTime','Creator'])
    
    return control_info



def insert_betradar_data(ID,config_number):
    try:
        # config_number = "2"
        # ID = '35080391'
        config_info = API_method.get_config_info(config_number = config_number)
    
        engine_list = create_connect(config_number = config_number)
        database_user = config_info['database_user']
        
        match_DF = pd.read_sql(f'select * from game_match where GameID = {ID}',con = engine_list[1])
        match_id = match_DF['DataSourceEventID'][0]+ ":" + str(match_DF['Producer'][0])
        print(match_id)
        game_league_ID = pd.read_sql('select LeagueID from game_league where LanguageCode = "zh-cn" ', con =engine_list[1])
        
        game_league_ID = game_league_ID['LeagueID'].tolist()
        
        betradar_address = os.path.abspath(os.getcwd()) + "/"
        
        path = betradar_address +'Error.txt'
        a_day = dt.timedelta(days = 1)
        creator = database_user
        # match_id = 'sr:match:33744357:1'
        # match_id = ID
        today = pd.to_datetime(dt.datetime.now())
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
            # continue
            dispose_engine(engine_list)
            return 'error'
        
        if ID_list[0] == 'sr:match':
            match_data = get_fixture_match(series = [ID_list[1]])
            match_data.to_sql(name = 'sport_events_fixture_sr_match',con = engine_list[0],if_exists = 'append',index = False)
    
            match_game_team_list, match_game_team, match_game_match = get_game_match_data(targetDF = match_data,error_path = path,config_number = config_number )
            for team_str in match_game_team["TeamName"]:
                if string_mapping(team_str) == True:
                    redis_time = pd.to_datetime(dt.datetime.now())
                    redis_input = [match_id,f'error: ({team_str}),TeamName error',redis_time,1]
                    redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                    redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                    dispose_engine(engine_list)
                    return 'error TeamName error'
                    # continue
            match_game_match['Producer'] = int(ID_list[2])
            delete_update_to_sql(insert_DF = match_game_team_list, table_name = 'game_team_list', engine = engine_list[1])
            update_duplicate_to_sql(insert_DF = match_game_team, table_name = 'game_team', engine = engine_list[1])
            delete_update_to_sql(insert_DF = match_game_match, table_name = 'game_match', engine = engine_list[1])
            
            if match_game_match['LeagueID'].unique()[0] in game_league_ID:
                pass
            else:
                new_game_league = update_game_league(engine_list,config_number = config_number)
    
                new_game_league_ID = new_game_league['LeagueID'].drop_duplicates().tolist()
                
                game_league_ID.extend(new_game_league_ID)
                
            redis_time = pd.to_datetime(dt.datetime.now())
            redis_input = [match_id,'complete',redis_time,0]
            redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
            redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)        
            dispose_engine(engine_list)
        
        elif ID_list[0] == 'sr:season':
            match_data = get_fixture_season(series = [ID_list[1]])
            if match_data.shape[0] == 0:
                redis_time = pd.to_datetime(dt.datetime.now())
                redis_input = [match_id,'warnning: no team for the season',redis_time,0]
                redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
                redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
                dispose_engine(engine_list)
                return 'error'
            match_data.to_sql(name = 'sport_events_fixture_sr_season',con = engine_list[0],if_exists = 'append',index = False)
            
            match_game_team_list, match_game_team, match_game_match = get_game_season_data(targetDF = match_data,error_path = path,config_number=config_number)
        
            match_game_match['Producer'] = int(ID_list[2])
            delete_update_to_sql(insert_DF = match_game_team_list, table_name = 'game_team_list', engine = engine_list[1])
            update_duplicate_to_sql(insert_DF = match_game_team, table_name = 'game_team', engine = engine_list[1])
            delete_update_to_sql(insert_DF = match_game_match, table_name = 'game_match', engine = engine_list[1])
            
            if match_game_match['LeagueID'].unique()[0] in game_league_ID:
                pass
            else:
                new_game_league = update_game_league(engine_list,config_number = config_number)
                
                new_game_league_ID = new_game_league['LeagueID'].drop_duplicates().tolist()
                
                game_league_ID.extend(new_game_league_ID)
                        
            redis_time = pd.to_datetime(dt.datetime.now())
            redis_input = [match_id,'complete',redis_time,0]
            redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
            redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
    
            dispose_engine(engine_list)
        else:
            redis_time = pd.to_datetime(dt.datetime.now())
            redis_input = [match_id,'error: ID incorrect format '+str(ID_list)+' ,please check.',redis_time,1]
            redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
            redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)

            dispose_engine(engine_list)
    except Exception as e:
        match_DF = pd.read_sql(f'select * from game_match where GameID = {ID}',con = engine_list[1])
        if match_DF.loc[0,'DataSourceEventID'] == None:
            if len(str(ID)) == 8:
                match_DF['DataSourceEventID'] = 'sr:match:'+str(ID)
        
        match_id = match_DF['DataSourceEventID'][0]+ ":" + str(match_DF['Producer'][0])
        redis_time = pd.to_datetime(dt.datetime.now())
        redis_input = [match_id,'error: '+str(e),redis_time,1]
        redis_input = pd.DataFrame([redis_input],columns = ['GameID','Status','CreateTime','Recover'])
        redis_input.to_sql(name = 'game_input_history',con = engine_list[0],if_exists = 'append',index = False)
    
        dispose_engine(engine_list)
        
def string_similar(s1, s2):
    return difflib.SequenceMatcher(None, s1, s2).quick_ratio()


def create_error_team():
    error_team_list = []
    error_key = ['R16P','Qf','Wqf','R64P','R32P']
    for i in error_key:
        for j in range(1,70):
            error_team = i+str(j)
            error_team_list.append(error_team)
            # print(error_team)
        
    return error_team_list

def string_mapping(s1):
    error_team_list = create_error_team()
    mp_list = list(map(lambda x:string_similar(s1,x), error_team_list))
    
    # [i for i in mp_list if i >= 0.5]
    
    if len([i for i in mp_list if i >= 0.6]) != 0:
        return True
    else:
        return False


    
def update_team_name(TeamID,config_number):
    LanguageCode = 'zh-cn'
    # TeamID = '315825'
    # config_number = '1'
    engine_list = create_connect(config_number = config_number)
    config_info = API_method.get_config_info(config_number = config_number)
    database_user = config_info['database_user']
    update_TeamDF = pd.read_sql(f"""select * from game_team where TeamID = "{TeamID}" and LanguageCode = "{LanguageCode}"  """,con = engine_list[1])
    
    url = f'https://stgapi.betradar.com/v1/sports/zh/competitors/sr:competitor:{TeamID}/profile.xml'

    headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
    response = requests.get(url,headers = headers)
    # DF = pd.read_xml(path_or_buffer =response.text,parser = 'etree')
    data_text = response.text
    data_text = data_text.replace("\'",'^')
    data_text = repr(data_text)
    data_text = data_text.replace("\'",'')
    data_text = data_text.replace("\\",'/')

    data_text = data_text.replace('，',' ')
    obj = xmltodict.parse(data_text)
    aaa = json.dumps(obj)
    aaa = json.loads(aaa)

    TeamName = aaa['competitor_profile']['competitor']['@name']
    
    update_TeamDF['TeamName'] = TeamName
    today = pd.to_datetime(dt.datetime.now())
    update_TeamDF['UpdateTime'] = today
    update_TeamDF['Updator'] = database_user

    update_duplicate_to_sql(insert_DF = update_TeamDF, table_name = 'game_team', engine = engine_list[1])
    dispose_engine(engine_list)
    return f"{TeamID} update success"

def get_league_json(league_id):
    url = f'https://stgapi.betradar.com/v1/sports/zh/tournaments/sr:tournament:{league_id}/info.xml'
    
    headers = {'x-access-token':'bPEq3Md3Efmqzq5wyq'}
    response = requests.get(url,headers = headers)
    obj = xmltodict.parse(response.text)
    aaa = json.dumps(obj)
    aaa = json.loads(aaa)
    return aaa['tournament_info']['tournament']

def check_betradar_data(GameID,config_number):
    
    # GameID = '35375689'
    # target = 'team'
    # config_number = '2'
    engine_list = create_connect(config_number = config_number)
    check_DF = pd.read_sql(f'select * from game_match where GameID = {GameID}', con = engine_list[1])
    league_id = check_DF['LeagueID'][0]
    database_league_name = pd.read_sql(f"""select LeagueName from game_league where LeagueID = {league_id} and LanguageCode = "zh-cn" """, con = engine_list[1])
    database_league_name = database_league_name['LeagueName'][0]
    match_id = check_DF['DataSourceEventID'][0]
    
    match_result = fixture_match_api(match_id)
    match_league_json = match_result['fixtures_fixture']['fixture']['tournament']
    betradar_tournament_info = get_league_json(league_id)
    
    database_json = {'LeagueID':str(league_id),'LeagueName':database_league_name} 
    
    return_league_json = {'Database':database_json, 'betradar_fixture':match_league_json, 'betradar_tournament_info':betradar_tournament_info}
    
    databae_team_DF = pd.read_sql(f'select * from game_team_list where GameID = {GameID}', con = engine_list[1])
    # Team_ID_list = list(databae_team_DF['TeamID'])
    # TeamID = 882449
    database_team_json = {}
    betradar_competitors_json = {}
    for i in range(0,databae_team_DF.shape[0]):
        # i = 1
        TeamID = databae_team_DF.loc[i,'TeamID']
        Team_Type = databae_team_DF.loc[i,'Type']
        Team_Name = pd.read_sql(f"""select TeamName from game_team where TeamID = {TeamID} and LeagueID = {league_id} and LanguageCode = "zh-cn" """, con = engine_list[1])
        Team_Name = Team_Name['TeamName'][0]
        Team_json = {str(Team_Type):{'TeamID':str(TeamID),'Team_Name':Team_Name}}
        database_team_json = database_team_json | Team_json
        
        betradar_competitors_data = get_team_api(str(TeamID))
        betradar_competitors_data = betradar_competitors_data['competitor_profile']['competitor']
        betradar_competitors_data = {str(Team_Type):betradar_competitors_data}
        betradar_competitors_json = betradar_competitors_json | betradar_competitors_data
        
    match_Team_json = match_result['fixtures_fixture']['fixture']['competitors']
    
    return_team_json = {'Database':database_team_json, 'betradar_fixture':match_Team_json,'betradar_competitors':betradar_competitors_json}
    
    All_return = {'League':return_league_json,'Team':return_team_json}
    
    return All_return


# def lost_event():
    