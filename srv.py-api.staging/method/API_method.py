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

def get_operating_system(config_number = "2"):
    
    config = configparser.ConfigParser()

    config.read('config.ini')
    
    config_name = 'operating_system'
    
    config_info = config[config_name]
    
    system_name = config_info['os']
    # config_info['env_name']
    return system_name

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


def control_api_create_user(IP,user_name, password, path,env_name,encode,system_name,position,config_number):
        try:
            # config_number = '1'
            engine_list = btd.create_connect(config_number = config_number)
            engine = engine_list[0]
            path = path.replace('\\','/')
            create_user(IP,user_name, password, path,env_name,engine,system_name,position,encode = encode)
            # create_user(IP, user_name, password, path, env_name, engine, system_name, position)
            btd.dispose_engine(engine_list)
            return {'IP':IP,'UserName':user_name,'Status':'complete'}
        
        except:
            btd.dispose_engine(engine_list)
            return {'IP':IP,'UserName':user_name,'Status':'error'}



def control_api_update_user(IP,user_name, password, update_column,update_value,config_number):
    try:
        # IP = '10.8.0.6'
        # user_name = 'pkwin'
        # update_column = 'EnvName'
        # update_value = 'company_env'
        # password = 'PKwin111'
        engine_list = btd.create_connect(config_number = config_number)
        engine = engine_list[0]
        if update_column == 'Path':
            update_value = update_value.replace('\\','/')
        
        update_user(IP,user_name, password, update_column, update_value,engine)
        btd.dispose_engine(engine_list)
        return {'IP':IP,'UserName':user_name,'Status':'complete'}
    
    except:
        btd.dispose_engine(engine_list)
        return {'IP':IP,'UserName':user_name,'Status':'error'}
    
    
def control_api_delete_user(IP,user_name,password,config_number):
    try:
        # IP = '10.9.9.9'
        # user_name = 'aaa'
        # password = 'bbb'
        engine_list = btd.create_connect(config_number = config_number)
        engine = engine_list[0]
        delete_user(IP,user_name,password,engine)
        btd.dispose_engine(engine_list)
        return {'IP':IP,'UserName':user_name,'Status':'complete'}
    
    except:
        btd.dispose_engine(engine_list)
        return {'IP':IP,'UserName':user_name,'Status':'error'}



def control_api_open_api(IP,config_number):
    local_ip = get_IP()
    # config_number = '2'
    # local_ip = '192.168.254.133'
    # user_name = 'makise'
    # job_name = 'app.py'
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    
    # if IP != local_ip:
        
    user_info = get_user_info(IP, engine)
    user_name = user_info['UserName'][0]
    Codepath = user_info['Path'][0]
    
    password = user_info['Password'][0]
    password = decode_password(password)
    env_name = user_info['EnvName'][0]
    system_name = user_info['System'][0]
    config_info = get_config_info(config_number = config_number)
    conda_file_name = config_info['conda_file_name']
    anaconda_path = Codepath.rsplit('/',1)[0]+"/"+conda_file_name+"/bin"
    if system_name == 'windows':
        drive_name = Codepath.split(':')[0] + ':'
        if env_name == "None":
            command = f"""{drive_name} & cd {Codepath} & python app.py {config_number}"""
        else:
            command = f"""activate {env_name} & {drive_name} & cd {Codepath} & python app.py {config_number}"""
            
    elif system_name == 'linux':
        # anaconda_path = "/home/"+user_info['UserName'][0]+"/"+conda_file_name+"/bin"
        if env_name == "None":

            command = f'cd {Codepath} && python3 app.py {config_number}'
        else:
            command = f'cd {anaconda_path} && source activate {env_name} && cd {Codepath} && python3 app.py {config_number}'
            # command = f'source activate {env_name} && cd {Codepath} && python3 app.py {config_number}'
    if IP == local_ip:
        
        app_start_job('app', config_number = config_number)
    else:
        return_code = ssh_command(IP,22,user_name,password,command)
    # Codepath = os.path.abspath(os.path.join(os.getcwd(), os.path.pardir))
    # cmd = f"start cmd /b /c python "+ Codepath +"\\"+ job_name+".py"
    time.sleep(3)
    btd.dispose_engine(engine_list)
    return "Job start"
    # else:
    #     user_info = get_user_info(IP, engine)
    #     user_name = user_info['UserName'][0]
        
    #     Codepath = user_info['Path'][0]
    #     system_name = user_info['System'][0]
    #     password = user_info['Password'][0]
    #     password = decode_password(password)
    #     env_name = user_info['EnvName'][0]
    #     config_info = get_config_info(config_number = config_number)
    #     conda_file_name = config_info['conda_file_name']
        
    #     if system_name == 'windows':
    #         drive_name = Codepath.split(':')[0] + ':'
    #         if env_name == "None":
    #             # command = f"""start cmd /b /c "{drive_name} & cd {Codepath} & python app.py {config_number}" """
    #             command = f"""start cmd /b /c "{drive_name} & cd {Codepath} & python app.py {config_number}" """
    #         else:
    #             # command = f"""activate {env_name} & start cmd /b /c "{drive_name} & cd {Codepath} & python app.py {config_number}" """
    #             command = f"""start cmd /b /c "activate {env_name} & {drive_name} & cd {Codepath} & python app.py {config_number}" """
                
    #     elif system_name == 'linux':
    #         anaconda_path = "/home/"+user_info['UserName'][0]+"/"+conda_file_name+"/bin"
    #         if env_name == "None":

    #             command = f'cd {Codepath} && python3 app.py {config_number}'
    #         else:
    #             command = f'cd {anaconda_path} && source activate {env_name} && cd {Codepath} && python3 app.py {config_number}'
    #     # return_code = ssh_command(IP,22,user_name,password,command)
    #     os.system(command)
    #     # return_code = API_method.ssh_command(IP,22,user_name,password,command)
    #     time.sleep(3)
    #     return "Job start"
    
def control_api_close_api(IP,config_number):
    # btd,API_method = init_method()
    # import method.betradar_data as btd
    # import method.API_method as API_method
    # user_name = 'pkwin'
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    local_ip = get_IP()
    # IP = get_IP()
    # user_name = getpass.getuser()
    user_info = get_user_info(IP, engine_list[0])
    user_name = user_info['UserName'][0]

    system_name = user_info['System'][0]
    app_PID_DF =pd.read_sql(f"""select * from betradar_API_info where IP = "{IP}" and Job = "app.py"; """,con = engine)
    # app_PID_DF.tolist()
    if app_PID_DF.shape[0] == 0:
        return f"App {IP}/app.py not exist"
    
    app_PID = app_PID_DF['PID'][0]
    # app_PID = 9132
    # local_ip = get_IP()
    
    # checkJob =pd.read_sql(f"""select * from betradar_API_info where IP = "{IP}" and PID = {app_PID}""",con = engine)

    if system_name == 'linux':
        cmd = f"kill -s 9 {app_PID}"
    if system_name == 'windows':
        cmd = f"taskkill /f /t /pid {app_PID}"
    
    if IP == local_ip:
        r = os.system(cmd)
        engine.execute(sa_text(f'''delete from betradar_API_info where IP = "{IP}" and PID = {app_PID} ''').execution_options(autocommit=True))
        engine.dispose()
    
    else:
        password = decode_password(user_info['Password'][0])
        return_code = ssh_command(IP,22,user_name,password,cmd)
        engine.execute(sa_text(f'''delete from betradar_API_info where IP = "{IP}" and PID = {app_PID} ''').execution_options(autocommit=True))
        engine.dispose()
    
    btd.dispose_engine(engine_list)
    return f"App {IP}/{app_PID} kill"


    
def control_api_stop_job(IP,PID,config_number):
    # IP = '192.168.19.129'
    # PID = '671869'
    # local_ip = API_method.get_IP()
    # if IP != local_ip:
        # print('aaa')
    # engine = engine_list[0]
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    config_info = get_config_info(config_number = config_number)

    checkJob =pd.read_sql(f"""select * from betradar_API_info where IP = "{IP}" and PID = {PID}""",con = engine)
    if checkJob.shape[0] == 0:
        # pass
        btd.dispose_engine(engine_list)
        return f"Job {IP}/{PID} not exist"
    
    else:
        
        job_port = config_info['job_api_port']

        r = requests.delete(f"""http://{IP}:{job_port}/api_control/stop_job/{PID}""")
        
        engine.execute(sa_text(f'''delete from betradar_API_info where IP = "{IP}" and PID = {PID} ''').execution_options(autocommit=True))
        engine.dispose()
        btd.dispose_engine(engine_list)
        return f"Job {IP}/{PID} kill"
    

    
def control_api_start_job(IP,job_name,job_num,config_number):
    config_info = get_config_info(config_number = config_number)

    job_port = config_info['job_api_port']

    r = requests.post(f"""http://{IP}:{job_port}/api_control/start_job/{job_name}?job_num={job_num}""")
    # r = requests.post('http://192.168.254.133:5369/api_control/start_job/error_recover?job_num=1')
    return({"response":str(r)})


    
def app_start_job(job_name,config_number):
    engine_list = btd.create_connect(config_number = config_number)

    config_info = get_config_info(config_number = config_number)
    IP = get_IP()
    # user_name = getpass.getuser()
    # IP = '192.168.254.133'
    # user_name = 'makise'
    # job_name = 'lost_data_redis'
    # try:
    user_info = get_user_info(IP, engine_list[0])
    # except:
    #     pass
    # user_name = user_info['UserName'][0]
    # config.read('config.ini')
    # if system_name == None:
    system_name = user_info['System'][0]
    # if Codepath == None:
    Codepath = user_info['Path'][0]
    # Codepath = '/home/jackey/betradar'
    # job_name = 'lost_data_redis'
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
    
    user_info = get_user_info(IP, engine_list[0])
    user_name = user_info['UserName'][0]
    system_name = user_info['System'][0]

    # local_ip = get_IP()
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


def control_api_auto_start_job(job_name,config_number):
    # config_number = '2'
    engine_list = btd.create_connect(config_number = config_number)
    config_info = get_config_info(config_number = config_number)
    engine = engine_list[0]
    UserDF =pd.read_sql(f"""select * from betradar_API_user """,con = engine)
    JobDF = pd.read_sql(f"""select * from betradar_API_info """,con = engine)
    PositionDF = UserDF[['IP','Position']]
    JobDF = pd.merge(PositionDF,JobDF,how = 'left', on = 'IP')
    JobDF = JobDF.loc[JobDF['Position'] == 'worker']
    
    JobDF = JobDF.groupby('IP')[['Job']].count().reset_index(drop = False)
    if JobDF.shape[0] != 0:
        
        worker_IP = JobDF.loc[JobDF['Job'].idxmin(),'IP']
        
        control_api_start_job(IP = worker_IP,job_name = job_name,job_num = 1,config_number = config_number)
        btd.dispose_engine(engine_list)
    else:
        btd.dispose_engine(engine_list)
        return "No worker"
    
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
    
def control_api_check_job_exist(config_number):
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    config_info = get_config_info(config_number = config_number)
    job_port = config_info['job_api_port']
    local_ip = get_IP()
    # local_ip = '192.168.254.132'
    job_DF = pd.read_sql(f"""select * from betradar_API_info where Job !="control_app.py" """,con = engine)
    
    check_ip_list = list(job_DF['IP'].drop_duplicates())
    
    for check_ip in check_ip_list:
        try:
            r = requests.post(f"""http://{check_ip}:{job_port}/api_control/check_job""")
        except:
            sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{check_ip}" and Job = "app.py";"""
            engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
        
    btd.dispose_engine(engine_list)

def control_api_cluster_restart_app(config_number):
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    config_info = get_config_info(config_number = config_number)
    job_port = config_info['job_api_port']
    user_DF = pd.read_sql(f"""select * from betradar_API_user where Position = "worker" """,con = engine)
    
    for i in range(0,user_DF.shape[0]):
        # i = 0
        IP = user_DF.loc[i,'IP']
        # user_name = user_DF.loc[i,'UserName']
        r = requests.post(f"""http://{IP}:{job_port}/api_control/restart_api""")
        # control_api_close_api(IP,config_number)
        # time.sleep(1)
        # control_api_open_api(IP,config_number)
    btd.dispose_engine(engine_list)
def control_api_restart_app(IP,config_number):
    config_info = get_config_info(config_number = config_number)
    job_port = config_info['job_api_port']
    
    r = requests.post(f"""http://{IP}:{job_port}/api_control/restart_api""")
    # control_api_close_api(IP,config_number)
    # time.sleep(1)
    # control_api_open_api(IP,config_number)
        

def control_api_sync_code(IP,config_number):
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    config_info = get_config_info(config_number = config_number)
    # job_port = config_info['job_api_port']

    user_DF = pd.read_sql(f"""select * from betradar_API_user where Position = "worker" """,con = engine)
    user_info = user_DF.loc[user_DF['IP'] == IP]
    user_info.reset_index(drop = True,inplace = True)
    # for i in range(0,user_DF.shape[0]):
    # i = 0
    IP = user_info.loc[0,'IP']
    user_name = user_info.loc[0,'UserName']
    password = decode_password(user_info.loc[0,'Password'])
    system_name = user_info.loc[0,'System']
    Codepath = user_info.loc[0,"Path"]
    conda_file_name = config_info['conda_file_name']
    env_name = user_info.loc[0,"EnvName"]
    anaconda_path = Codepath.rsplit('/',1)[0]+"/"+conda_file_name+"/bin"
    if system_name == 'windows':
        drive_name = Codepath.split(':')[0] + ':'
        if env_name == "None":
            command = f"""{drive_name} & cd {Codepath} & python sync_code.py {config_number}"""
        else:
            command = f"""activate {env_name} & {drive_name} & cd {Codepath} & python sync_code.py {config_number}"""
            
    elif system_name == 'linux':
        # anaconda_path = "/home/"+user_name+"/"+conda_file_name+"/bin"
        if env_name == "None":

            command = f'cd {Codepath} && python3 sync_code.py {config_number}'
        else:
            command = f'cd {anaconda_path} && source activate {env_name} && cd {Codepath} && python3 sync_code.py {config_number}'
    
    ssh_command(IP,22,user_name,password,command)
    btd.dispose_engine(engine_list)

def control_api_cluster_sync_code(config_number):
    
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    config_info = get_config_info(config_number = config_number)
    # job_port = config_info['job_api_port']

    user_DF = pd.read_sql(f"""select * from betradar_API_user where Position = "worker" """,con = engine)
    # user_info = user_DF.loc[user_DF['IP'] == IP]
    # user_info.reset_index(drop = True,inplace = True)
    for i in range(0,user_DF.shape[0]):
    # i = 0
        IP = user_DF.loc[i,'IP']
        # print(IP)
        user_name = user_DF.loc[i,'UserName']
        password = decode_password(user_DF.loc[i,'Password'])
        system_name = user_DF.loc[i,'System']
        Codepath = user_DF.loc[i,"Path"]
        conda_file_name = config_info['conda_file_name']
        env_name = user_DF.loc[i,"EnvName"]
        anaconda_path = Codepath.rsplit('/',1)[0]+"/"+conda_file_name+"/bin"
        if system_name == 'windows':
            drive_name = Codepath.split(':')[0] + ':'
            if env_name == "None":
                command = f"""{drive_name} & cd {Codepath} & python sync_code.py {config_number}"""
            else:
                command = f"""activate {env_name} & {drive_name} & cd {Codepath} & python sync_code.py {config_number}"""
                
        elif system_name == 'linux':
            # anaconda_path = "/home/"+user_name+"/"+conda_file_name+"/bin"
            if env_name == "None":
    
                command = f'cd {Codepath} && python3 sync_code.py {config_number}'
            else:
                command = f'cd {anaconda_path} && source activate {env_name} && cd {Codepath} && python3 sync_code.py {config_number}'
        
        ssh_command(IP,22,user_name,password,command)
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

def control_api_cluster_stop_all_job(config_number):
    engine_list = btd.create_connect(config_number = config_number)
    engine = engine_list[0]
    config_info = get_config_info(config_number = config_number)
    job_port = config_info['job_api_port']
    user_DF = pd.read_sql(f"""select * from betradar_API_user where Position = "worker" """,con = engine)
    for IP in user_DF['IP']:
        
        r = requests.delete(f"""http://{IP}:{job_port}/api_control/stop_all_job""")
    btd.dispose_engine(engine_list)
        # engine.execute(sa_text(f'''delete from betradar_API_info where IP = "{IP}" and PID = {PID} ''').execution_options(autocommit=True))
        # engine.dispose()
        # print(IP)
        
def app_translate_team(config_number,team_name,translate_team_name):
    engine_list = btd.create_connect(config_number = config_number)

    config_info = get_config_info(config_number = config_number)
    IP = get_IP()
    # IP = '192.168.1.243'
    # config_number = '1'
    system_name = config_info['system']
    
    Codepath = config_info['code_path']
    env_name = config_info['env_name']
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
    system_name = config_info['system']
    
    Codepath = config_info['code_path']
    env_name = config_info['env_name']
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