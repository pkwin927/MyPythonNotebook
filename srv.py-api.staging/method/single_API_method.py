# -*- coding: utf-8 -*-
"""
Created on Wed May 25 17:26:08 2022

@author: User
"""

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Random import get_random_bytes
import base64
import paramiko
import pandas as pd
import os
import ast
import method.betradar_data as btd
from sqlalchemy.sql import text as sa_text
import json
import netifaces
from netifaces import interfaces, ifaddresses, AF_INET
import configparser
from paramiko import SSHClient
from scp import SCPClient
import time
import requests
import getpass
import logging
# from scp import SCPClient


# env_file_name = "betradar"

# env_file_name = 'betradar'


def get_IP():
    ifaceName = netifaces.gateways()['default'][2][1]
    IP = [i['addr'] for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr':'No IP addr'}] )][0]
    return IP

def ssh_command(hostname,port,username,password,command):
    # try:
    # hostname = IP
    # port = 22
    # username = 'makise'
    # password = password
    # command = 'activate company_env & C: & cd C:/Users/pkwin/Code/betradar_linux & python app.py 2'
    # command = 'source ~/.bashrc && source activate company && cd /home/makise/betradar_linux && python3 app.py 2'
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    # client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:  
        client.connect(hostname, port, username, password)
    except:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname, port, username, password)
    # command = "dir"
    stdin,stdout,stderr = client.exec_command(command)
    # print(stderr.readlines())

    # print(bbb)
    #     return 1
    # except:
    #     return 0
    

def get_user_info(IP,engine):
    user_info = pd.read_sql(f"""select * from betradar_API_user where IP = "{IP}" """,con = engine)
    return user_info


def create_AES_key(password,key_path,key_name = 'AES_key',file_extension = 'bin'):
    keyPath = key_path+'/' + key_name + '.'+file_extension
    iv_path = key_path+'/' + key_name+'_iv' + '.'+file_extension
    salt = get_random_bytes(32)
    
    AES_key = PBKDF2(password, salt, dkLen=32)
    
    cipher_AES = AES.new(AES_key, AES.MODE_CFB)
    
    with open(iv_path, "wb") as f:
        f.write(cipher_AES.iv)
    
    with open(keyPath, "wb") as f:
        f.write(AES_key)

    # 讀取金鑰
    with open(keyPath, "rb") as f:
        keyFromFile = f.read()
    
    # 檢查金鑰儲存
    assert AES_key == keyFromFile, '金鑰不符'

def AES_encode(data, key_path, key_name = 'AES_key',file_extension = 'bin'):
    # data = 'PKwin111'
    keyPath = key_path+'/' + key_name + '.'+file_extension
    iv_path = key_path+'/' + key_name+'_iv' + '.'+file_extension
    with open(keyPath, "rb") as f:
        AES_key = f.read()
        
    with open(iv_path, "rb") as f:
        iv = f.read()
    
    data_utf8 = data.encode()

    cipher_AES = AES.new(AES_key, AES.MODE_CFB, iv=iv)
    
    cipheredData = cipher_AES.encrypt(data_utf8)
    # with open(outputFile, "wb") as f:
    #     f.write(cipher.iv)
    #     f.write(cipheredData)
    return cipheredData

def create_RSA_key(key_path,key_name = 'RSA',file_extension = 'pem'):
    private_keyPath = key_path+'/' + key_name+'_private' + '.'+file_extension
    public_keyPath = key_path+'/' + key_name+'_public' + '.'+file_extension

    key = RSA.generate(2048)

    privateKey = key.export_key()
    with open(private_keyPath, "wb") as f:
        f.write(privateKey)

    publicKey = key.publickey().export_key()
    with open(public_keyPath, "wb") as f:
        f.write(publicKey)
        
def RSA_encode(data,key_path, key_name = 'RSA',file_extension = 'pem'):
    public_keyPath = key_path+'/' + key_name+'_public' + '.'+file_extension
    publicKey = open(public_keyPath).read()
    rsakey = RSA.importKey(publicKey)
    cipher = Cipher_pkcs1_v1_5.new(rsakey)     #創建用于執行pkcs1_v1_5加密或解密的密碼
    cipher_text = base64.b64encode(cipher.encrypt(data))
    return cipher_text.decode('utf-8')

def AES_decode(data,key_path, key_name = 'AES_key',file_extension = 'bin'):
    keyPath = key_path+'/' + key_name + '.'+file_extension
    iv_path = key_path+'/' + key_name+'_iv' + '.'+file_extension
    with open(keyPath, "rb") as f:
        AES_key = f.read()
    
    with open(iv_path, "rb") as f:
        iv = f.read()
    
    cipher = AES.new(AES_key, AES.MODE_CFB, iv=iv)

    originalData = cipher.decrypt(data)

    return originalData.decode()

def RSA_decode(data,key_path,key_name = 'RSA',file_extension = 'pem'):
    private_keyPath = key_path+'/' + key_name+'_private' + '.'+file_extension
    privateKey = open(private_keyPath).read()
    rsakey = RSA.importKey(privateKey)
    cipher = Cipher_pkcs1_v1_5.new(rsakey) #建立用於執行pkcs1_v1_5加密或解密的密碼
    text = cipher.decrypt(base64.b64decode(data),"解密失敗")
    return text

def create_key(password,key_path = None):
    if key_path is None:
        key_path = os.path.abspath(os.getcwd())+ '/key'
        
    create_AES_key(password,key_path)
    create_RSA_key(key_path)

def encode_password(data,key_path = None):
    if key_path is None:
        key_path = os.path.abspath(os.getcwd()) + '/key'
    data = AES_encode(data, key_path, key_name = 'AES_key',file_extension = 'bin')

    data = RSA_encode(data,key_path, key_name = 'RSA',file_extension = 'pem')
    
    return data

def decode_password(data,key_path = None):
    if key_path is None:
        key_path = os.path.abspath(os.getcwd())+ '/key'
    data = RSA_decode(data,key_path,key_name = 'RSA',file_extension = 'pem')

    data = AES_decode(data,key_path, key_name = 'AES_key',file_extension = 'bin')
    
    return data

def create_user(IP,user_name,password,path,env_name,engine,system_name,position,encode = True):
    encode = str(encode)
    encode = ast.literal_eval(encode)
    # path = path+"/" + env_file_name
    if encode is True:
        password = encode_password(password)
    else:
        pass
    user_info = pd.DataFrame([[IP,user_name,password,path,env_name,system_name,position]],columns = ['IP','UserName','Password','Path','EnvName','System','Position'])
    
    user_info.to_sql(name = 'betradar_API_user',con = engine,if_exists = 'append',index = False)

# def create_user(IP,user_name,password,path,engine,encode = True):
#     encode = str(encode)
#     encode = ast.literal_eval(encode)
#     if encode is True:
#         password = encode_password(password)
#     else:
#         pass
#     user_info = pd.DataFrame([[IP,user_name,password,path]],columns = ['IP','UserName','Password','Path'])
    
#     user_info.to_sql(name = 'betradar_API_user',con = engine,if_exists = 'append',index = False)

def update_user(IP,user_name,password,update_column,update_value,engine):
    
    DF = pd.read_sql(f"""select * from betradar_API_user where IP = "{IP}" and UserName = "{user_name}"; """,con = engine)
    check_password = DF['Password'][0]
    check_password = decode_password(check_password)
    if password == check_password:
        if update_column == 'Password':
            update_value = encode_password(update_value)
        sql_commend_str = f"""UPDATE betradar_API_user SET {update_column}="{update_value}" where IP = "{IP}" and UserName = "{user_name}"; """
        engine.execute(sa_text(sql_commend_str).execution_options(autocommit=True))
        # return {'delete':f""" {IP}/{user_name}"""}
    else:
        raise ValueError
    
    
def delete_user(IP,user_name,password,engine):
    # engine = engine_list[1]
    # IP = '10.8.0.6'
    # user = 'pkwin'
    DF = pd.read_sql(f"""select * from betradar_API_user where IP = "{IP}" and UserName = "{user_name}"; """,con = engine)
    check_password = DF['Password'][0]
    check_password = decode_password(check_password)
    if password == check_password:
        sql_commend_str = f"""DELETE FROM betradar_API_user where IP = "{IP}" and UserName = "{user_name}";"""
        engine.execute(sa_text(sql_commend_str).execution_options(autocommit=True))
        # return {'delete':f""" {IP}/{user_name}"""}
    else:
        raise ValueError
        # return {'error':'Password error'}
    
def get_open_job(config_number):
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    DF = pd.read_sql(f"""select * from betradar_API_info; """,con = engine)
    DF['CreateTime'] = DF['CreateTime'].astype(str)
    js = DF.to_json(orient = 'records')
    js = js.replace('\\\\',"/")
    js = json.dumps(js)
    js_result = json.loads(js)
    js_result = json.loads(js_result)
    # js_result = js_result.replace('/',"\\")
    btd.dispose_engine(engine_list)
    return js_result


# def ssh_scp_files(ssh_host, ssh_user, ssh_password, ssh_port, source_volume, destination_volume):
#     # logging.info("In ssh_scp_files()method, to copy the files to the server")
#     ssh = SSHClient()
#     ssh.load_system_host_keys()
#     ssh.connect(ssh_host, username=ssh_user, password=ssh_password, look_for_keys=False)

#     with SCPClient(ssh.get_transport()) as scp:

#         scp.put(source_volume, recursive=True,remote_path=destination_volume)


def get_config_info(config_number):
    # engine_list = btd.create_connect(config_number = config_number)

    # IP = get_IP()
    # User_name = os.getlogin()
    # user_info = get_user_info(IP, User_name, engine_list[0])
    
    # config_path = user_info['Path'][0]
    
    config = configparser.ConfigParser()

    config.read(f'config.ini')
    
    config_name = 'environment_'+config_number
    
    config_info = config[config_name]
    # config_info['env_name']
    return config_info

# def get_operating_system(config_number = "2"):
    
#     config = configparser.ConfigParser()

#     config.read('config.ini')
    
#     config_name = 'operating_system'
    
#     config_info = config[config_name]
    
#     system_name = config_info['system']
#     # config_info['env_name']
#     return system_name

def ssh_scp_files(ssh_host, ssh_user, ssh_password, source_path, local_path,ssh_port = 22,method = 'get'):
    # logging.info("In ssh_scp_files()method, to copy the files to the server")
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ssh_host,port = ssh_port, username=ssh_user, password=ssh_password, look_for_keys=False)
    # time.sleep(10)
    with SCPClient(ssh.get_transport()) as scp:
        if method == 'put':
            
            scp.put(source_path, recursive=True,remote_path=local_path)
        elif method == 'get':
            
            scp.get(source_path, recursive=True,local_path = local_path)
        

def get_position_info(engine, position):
    # engine = engine_list[0]
    # position = 'manager'
    position_DF = pd.read_sql(f"""select * from betradar_API_user where Position ="{position}"; """,con = engine)
    # position_list = position_DF['IP'].tolist()
    return position_DF
    
    
def check_job_exist(PID):
    try:
        os.kill(int(PID),0)
        return True
    except:
        return False
        

# def restart_app():
    



#####################################################################################################################


def app_check_job_exist(config_number):
    # config_number = '2'
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    local_ip = get_IP()
    # local_ip = '192.168.254.133'
    job_DF = pd.read_sql(f"""select * from betradar_API_info where IP = "{local_ip}" """,con = engine)
    
    PID_list = list(job_DF['PID'])
    
    for PID in PID_list:
        
        r = check_job_exist(PID)
        
        if r == False:
            
            sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and PID = "{PID}";"""
            engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
            # btd.dispose_engine(engine_list)
        # print(r)
    btd.dispose_engine(engine_list)
    
        

def app_stop_all_job(config_number):
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    local_ip = get_IP()
    # local_ip = "192.168.1.243"
    # config_number = '2'
    job_DF = pd.read_sql(f"""select * from betradar_API_info where IP = "{local_ip}" and Job != "app.py" and Job != "control_app.py" """,con = engine)

    for i in range(0,job_DF.shape[0]):
        # i = 0
        PID = job_DF.loc[i,'PID']
    
        app_stop_job(PID,config_number)
    btd.dispose_engine(engine_list)

        
def app_translate_team(config_number,team_name,translate_team_name):
    engine_list = btd.create_connect(config_number = config_number)

    config_info = get_config_info(config_number = config_number)
    IP = get_IP()
    # IP = '192.168.1.243'
    # config_number = '1'
    # user_info = get_user_info(IP, engine_list[0])

    system_name = config_info['System']
    
    Codepath = config_info['code_path']
    env_name = config_info['python_env_name']
    if system_name == 'linux':
        # cmd = "python3 "+ Codepath +"/"+ job_name+".py " + config_number + ' &'
        cmd = f'cd {Codepath} && python3 translate_team_name.py {config_number} "{team_name}" "{translate_team_name}" &'
    elif system_name == 'windows':
        drive_name = Codepath.split(':')[0] + ':'
        # cmd = "start cmd /b /c python "+ Codepath +"/"+ job_name+".py " + config_number
        cmd = f'{drive_name} & cd {Codepath} & start cmd /b /c python translate_team_name.py {config_number} "{team_name}" "{translate_team_name}"'
    r = os.system(cmd)
    btd.dispose_engine(engine_list)
    return r

def app_translate_league(config_number,league_name,translate_league_name):
    engine_list = btd.create_connect(config_number = config_number)

    config_info = get_config_info(config_number = config_number)
    IP = get_IP()
    # IP = '192.168.1.243'
    # config_number = '1'
    # user_info = get_user_info(IP, engine_list[0])

    system_name = config_info['System']
    
    Codepath = config_info['code_path']
    env_name = config_info['python_env_name']
    if system_name == 'linux':
        # cmd = "python3 "+ Codepath +"/"+ job_name+".py " + config_number + ' &'
        cmd = f'cd {Codepath} && python3 translate_league_name.py {config_number} "{league_name}" "{translate_league_name}" &'
    elif system_name == 'windows':
        drive_name = Codepath.split(':')[0] + ':'
        # cmd = "start cmd /b /c python "+ Codepath +"/"+ job_name+".py " + config_number
        cmd = f'{drive_name} & cd {Codepath} & start cmd /b /c python translate_league_name.py {config_number} "{league_name}" "{translate_league_name}"'
    r = os.system(cmd)
    btd.dispose_engine(engine_list)
    return r


def app_start_job(job_name,config_number):
    engine_list = btd.create_connect(config_number = config_number)

    config_info = get_config_info(config_number = config_number)
    IP = get_IP()
    Codepath = config_info['code_path']
    system_name = config_info['system']

    if system_name == 'linux':
        # cmd = "python3 "+ Codepath +"/"+ job_name+".py " + config_number + ' &'
        cmd = f'cd {Codepath} && python3 {job_name}.py {config_number} &'
    elif system_name == 'windows':
        drive_name = Codepath.split(':')[0] + ':'
        # cmd = "start cmd /b /c python "+ Codepath +"/"+ job_name+".py " + config_number
        cmd = f'{drive_name} & cd {Codepath} & start cmd /b /c python {job_name}.py {config_number}'
    r = os.system(cmd)
    btd.dispose_engine(engine_list)
    return "Job start"

def app_stop_job(PID,config_number):
    # IP = '192.168.1.243'
    # PID = '3884'
    # hostname = socket.gethostname()
    engine_list = btd.create_connect(config_number = config_number)
    IP = get_IP()
    config_info = get_config_info(config_number = config_number)
    Codepath = config_info['code_path']
    system_name = config_info['system']

    engine = engine_list[0]
    checkJob =pd.read_sql(f"""select * from betradar_API_info where IP = "{IP}" and PID = {PID}""",con = engine)
    if checkJob.shape[0] == 0:
        btd.dispose_engine(engine_list)
        return f"Job {IP}/{PID} not exist"
    if system_name == 'linux':
        cmd = f"kill -s 9 {PID}"
    if system_name == 'windows':
        cmd = f"taskkill /f /t /pid {PID}"
    r = os.system(cmd)
    engine.execute(sa_text(f'''delete from betradar_API_info where IP = "{IP}" and PID = {PID} ''').execution_options(autocommit=True))
    engine.dispose()
    btd.dispose_engine(engine_list)
    return f"Job {IP}/{PID} kill"