# -*- coding: utf-8 -*-

#位數不夠補零

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, event
from sqlalchemy.sql import text as sa_text

DF = pd.DataFrame({"A":[1,np.nan,1],
                   "B":[500,1000,2000],
                   "C":["AAA","BBB","CCC"],
                   "D":["VVV","FFF","DDD"],
                   "E":["123","345","456"]})



DF.dtypes
#pandas方法


DF["E"] = DF["E"].str.zfill(6)


#欄位type為數字
for i in range(0,DF.shape[0]):
    
    n = DF.loc[i,"A"]
    
    DF.loc[i,"A"] = "%05d" % n

DF.loc[1,"A"]

#欄位type為文字
for i in range(0,DF.shape[0]):
    
    n = DF.loc[i,"E"]
    
    DF.loc[i, "E"] = n.zfill(5)  

DF


rdd = DF.as_matrix()


rdd

## contains

DF.loc[DF["E"].str.contains("5",na = False),:]


##seaborn
##信賴區間作圖
import seaborn as sns

sns.factorplot("WatchTimes","IsDeal",col = "TimeRange",data = DF)

##精確度
df = pd.DataFrame({"A":[0.00001,0.25587,0.68598,0.4164],
                   "C":[0.147856,0.15423165,0.134554,0.1245]})

df.round({'A': 1, 'C': 2})


##笛卡兒積

import itertools
import numpy as np

RawDealRate = 0.5

KIDateDiff = 20

NCount = 10

LeftOverDay = 20

DealRateArray = np.arange(0.01,RawDealRate + 0.01,0.01)
KIDateDiffArray = np.arange(0,KIDateDiff+1,1)
NCountArray = np.arange(0,NCount+1,1)
LeftOverDayArray = np.arange(0,LeftOverDay+1,1)

DF = pd.DataFrame(list(itertools.product(DealRateArray,KIDateDiffArray,NCountArray,LeftOverDayArray)),columns = ["RawDealRate","KIDateDiff","NCount","LeftOverDay"])


##SQL ODBC

####################################抓資料#####################################
## SQL 帳號驗證
import pyodbc
import sys
import getpass
# Windows系統執行時打  python aaa.py 123456
#Linux系統執行時打     python aaa.py "123456" 

passwd = sys.argv[1] 

## 程式執行時輸入
passwd = getpass.getpass("Please input domain password:") #程式執行時輸入

passwd = "\""+str(passwd)+"\"" #Linux需接此段



cnxn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};'
                      r'SERVER=192.168.0.35;'
                      r'DATABASE=DWTVSale;'
                      r'UID=sa;'
                      r'PWD='+passwd)

###############################################################################


## Windows 驗證
cnxn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};'
                      r'SERVER=192.168.0.35;'
                      r'DATABASE=DWTVSale;'
                      r'Trusted_Connection=yes')

###############################################################################

##############################匯資料進SQL######################################

from sqlalchemy import create_engine, event

DF = pd.DataFrame({"A":[100,200,300],
                   "B":[500,1000,2000],
                   "C":["AAA","BBB","CCC"],
                   "D":["VVV","FFF","DDD"],
                   "E":["123","345","456"]})

#windows驗證
engine = create_engine("mssql+pyodbc://192.168.0.35/DWTVSale?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes",echo = False)

#SQL驗證

engine = create_engine("mssql+pyodbc://dw:aaaaaa@192.168.0.45/DWTVSale?driver=ODBC+Driver+17+for+SQL+Server",echo = False)

##快速匯入SQL
@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    print("FUNC call")
    if executemany:
        cursor.fast_executemany = True

DF.to_sql("TestData",con = engine, if_exists = "append", index = False)
## 如果資料庫有Schema要求

from sqlalchemy.sql import text as sa_text
## method1
##先在資料表中建立資料表並指定好Schema

engine = create_engine("mssql+pyodbc://192.168.0.45/DWTVSale?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes",echo = False)

connect = engine.connect()
## Truncate Table
engine.execute(sa_text('''TRUNCATE TABLE test''').execution_options(autocommit=True))

connect.close()

## use append
DF.to_sql("test",con = engine, if_exists = "append", index = False)

## method2

## use replace
DF.to_sql("test",con = engine, if_exists = "replace", index = False)

connect = engine.connect()
## Change SQL Table Schema
engine.execute(sa_text('''alter TABLE test ALTER COLUMN [E] nvarchar(50)''').execution_options(autocommit=True))

connect.close()






##unique函数去除其中重複的元素，并按元素由大到小返回一个新的無元素重複的元组或者列表

x = [0,0,0,1,1,1,2,2,2,3,3,3,4,4,4,4]

np.unique(x)

##今天日期

import datetime
import pandas as pd

today = pd.to_datetime(datetime.datetime.today().date())

#tommorrow = today + datetime.timedelta(days = 1) 

##時間加減一個月
from dateutil.relativedelta import relativedelta

test = today + relativedelta(months=5)


## 直接抓出數字最小或最大的該筆資料
DF = pd.DataFrame({"A":[100,200,300],
                   "B":[500,1000,2000],
                   "C":["AAA","AAA","AAA"],
                   "D":["VVV","FFF","DDD"],
                   "E":["123","345","456"]})
    
DF.reset_index(drop = True, inplace = True) # 有篩選的話，imdex要先reset

test = DF.iloc[DF.groupby(["C"]).apply(lambda x: x['A'].idxmin())]




