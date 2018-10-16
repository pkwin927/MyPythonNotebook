# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 16:17:37 2018

@author: jackey.chen
"""

import flask
app = flask.Flask(__name__)
#-------- MODEL GOES HERE -----------#

import numpy as np
import pandas as pd
from sklearn.externals import joblib

fileName = 'K:/Jackey/程式筆記/PythonCollection/程式範例/ModelWebAPIExample/MyRF/RFModelV5_1.sav'

print("load model")

load_model = joblib.load(fileName)

print("load over")
#-------- ROUTES GO HERE -----------#

@app.route('/predict', methods=["POST"])
def predict():
    
## Post DataFrame 
    
    content = flask.request.get_data(as_text = True)
    
    DF = pd.read_json(content)
         
    item = DF.copy()
 
    item["Age"] = item["Age"].astype('int64')
    
    item["ShortCallNCounts"] = item["ShortCallNCounts"].astype('int64')
    ## Prepare AgeRange
    
    item.loc[item["Age"] == 0,"AgeRange_AgeNull"] = 1
    
    item.loc[(item["Age"] > 0) & (item["Age"] < 19),"AgeRange_High_Student"] = 1
    
    item.loc[(item["Age"] >= 19) & (item["Age"] <= 24),"AgeRange_Bachelor_Master"] = 1
    
    item.loc[(item["Age"] >= 25) & (item["Age"] < 60),"AgeRange_Middle_Group"] = 1
    
    item.loc[(item["Age"] >= 60),"AgeRange_Old_Group"] = 1
    
    ## Prepare Gender
    
    item.loc[item["Sex"] == "man","Gender"] = 0
    item.loc[item["Sex"] == "girl","Gender"] = 1
    
    item.loc[item["StudyTarget"] == "SelfStudy","StudyTarget2_SelfStudy"] = 1
    
    item.loc[(item["StudyTarget"] == "StudyNull") | 
              (item["StudyTarget"] == ""),"StudyTarget2_StudyNull"] = 1
    
    item.loc[(item["StudyTarget"].str.contains("Parents")),"StudyTarget2_Parents"] = 1
    
    ## Prepare ShortCallNCount
    
    item.loc[item["ShortCallNCounts"] <= 2,"N_CountGroup_0~2N"] = 1
    
    item.loc[(item["ShortCallNCounts"]>=3) & (item["ShortCallNCounts"] <=8),"N_CountGroup_3~8N"] = 1
    
    item.loc[(item["ShortCallNCounts"] > 8),"N_CountGroup_Else"] = 1
    
    ## Prepare JobType
    
    item.loc[item["JobType"] == "","JobType2_IsNull"] = 1
    
    item.loc[item["JobType"] == "NoProvide","JobType2_NoProvide"] = 1
    
    item.loc[item["JobType"] == "Work","JobType2_Work"] = 1
    
    item.loc[item["JobType"] == "NoWork","JobType2_NoWork"] = 1
    
    item.loc[item["JobType"] == "Home","JobType2_Home"] = 1
    
    ## Prepare ProjectRoute
    
    item.loc[item["ProjectRoute"] == "億捷","ProjectRouteCover_lt"] = 1
    
    item.loc[item["ProjectRoute"] == "FB", "ProjectRouteCover_FB"] = 1
    
    item.drop(["Sex","Age","JobType","ProjectRoute","StudyTarget","ShortCallNCounts"],axis = 1, inplace = True)
    
    item.fillna(0, inplace = True)
    
    PredictData = item[['Gender', 'WatchType', 'AgeRange_AgeNull', 'AgeRange_Bachelor_Master',
                       'AgeRange_High_Student', 'AgeRange_Middle_Group', 'AgeRange_Old_Group',
                       'StudyTarget2_Parents', 'StudyTarget2_SelfStudy',
                       'StudyTarget2_StudyNull', 'N_CountGroup_0~2N', 'N_CountGroup_3~8N',
                       'N_CountGroup_Else', 'JobType2_Home', 'JobType2_IsNull',
                       'JobType2_NoProvide', 'JobType2_NoWork', 'JobType2_Work',
                       'ProjectRouteCover_FB', 'ProjectRouteCover_lt']]
    
    score = load_model.predict_proba(PredictData)
    
    score = pd.DataFrame(score,columns = ["NoDeal","Deal"])
    
    ResultData = pd.concat([DF,score],axis = 1)

## Write Data to SQL
    from sqlalchemy import create_engine, event

    engine = create_engine("mssql+pyodbc://192.168.0.45/DWTVSale?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes",echo = False)

    ResultData.to_sql("testJson",con = engine, if_exists = "replace", index = False)

## Return Json 
    ResultDataJson = ResultData.to_json(orient = "records",force_ascii = False)
    
    return(ResultDataJson)


if __name__ == '__main__':
    '''Connects to the server'''
    print("Start")
    
    HOST = '192.168.0.120'
    PORT = 4000      #make sure this is an integer
    app.run(HOST, PORT)





