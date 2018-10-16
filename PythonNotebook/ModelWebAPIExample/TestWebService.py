# -*- coding: utf-8 -*-
"""
Created on Mon Apr  2 09:20:41 2018

@author: jackey.chen
"""

import requests
import pandas as pd
import json
import pyodbc
import time

df = pd.DataFrame([["18330A0268","girl",1,"Work","FB","SelfStudy",30,2],
                   ["18330B0269","men",1,"Work","FB","Parents",48,10],
                   ["18330B0270","girl",2,"NoWork","FB","SelfStudy",35,11],
                   ["18330B0271","men",1,"Work","FB","SelfStudy",37,13],
                   ["18330B0272","girl",1,"Work","FB","SelfStudy",23,2],
                   ["18330B0273","men",1,"NoWork","FB","Parents",61,3],
                   ["18330B0274","girl",9,"Work","FB","SelfStudy",33,10],
                   ["18330B0275","girl",1,"Work","FB","SelfStudy",20,5],
                   ["18330B0276","men",1,"NoWork","FB","Parents",28,10],
                   ["18330B0277","men",2,"Work","FB","SelfStudy",24,7],
                   ["18330B0278","girl",1,"Work","FB","Parents",22,10],
                   ["18330B0279","men",1,"Work","FB","SelfStudy",27,12],
                   ["18330B0280","men",1,"Work","FB","SelfStudy",27,12],
                   ["18330B0281","men",9,"NoWork","FB","SelfStudy",27,12],
                   ["18330B0282","men",9,"NoWork","FB","SelfStudy",28,12],
                   ["18330B0283","men",9,"NoWork","FB","SelfStudy",28,12],
                   ["18330B0284","men",1,"Work","FB","SelfStudy",27,12],
                   ["18330B0285","men",1,"Work","FB","SelfStudy",28,12]],
                  columns=["CustomerCode","Sex","WatchType","JobType","ProjectRoute","StudyTarget","Age","ShortCallNCounts"])

#DF1 = df.to_json(orient = "values",force_ascii = False)

DF1 = df.to_json(orient = "records",force_ascii = False)

#DF2 = DF.to_json(orient = "split",force_ascii = False)

#DF3 = DF.to_json(orient = "index",force_ascii = False)

headers = {'content-type':'application/json',}
params = (("priority","normal"),)

## Post Data
r = requests.post("http://192.168.0.120:4000/predict", data = DF1,headers = headers)

ResultJson = r.text

## Json to DataFrame
DF = pd.read_json(ResultJson,precise_float = True)

DF[["CustomerCode","Deal","NoDeal"]]


## Timing
start = time.time()

r = requests.post("http://192.168.0.120:4000/predict", data = DF1,headers = headers)

end = time.time()
elapsed = end - start
print(elapsed)

print(r.text)

