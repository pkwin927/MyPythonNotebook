# -*- coding: utf-8 -*-
"""
Created on Tue May 31 09:37:40 2022

@author: User
"""
import configparser

config = configparser.ConfigParser()
config['environment_2'] = {}
test_environment = config['environment_2']
test_environment['env_name'] = 'Test_environment'
test_environment['database_IP'] = '192.168.1.239'
test_environment['database_port'] = '3306'
test_environment['database_user'] = 'jackey'
test_environment['database_password'] = '1q2w3e4r'
test_environment['redis_IP'] = '192.168.1.239'
test_environment['redis_port'] = '6379'
test_environment['redis_password'] = 'redispass123'
# test_environment['env_file_name'] = 'betradar'
test_environment['control_API_port'] = '1999'
test_environment['job_API_port'] = '5000'

###################################################################
config['environment_1'] = {}
formal_environment = config['environment_1']

formal_environment['env_name'] = 'Formal_environment'
formal_environment['database_IP'] = '18.163.118.239'
formal_environment['database_port'] = '3306'
formal_environment['database_user'] = 'jackey'
formal_environment['database_password'] = '1q2w3e4r'
formal_environment['redis_IP'] = '18.163.118.239'
formal_environment['redis_port'] = '6379'
# test_environment['redis_password'] = 'redispass123'
# formal_environment['env_file_name'] = 'betradar'
formal_environment['control_API_port'] = '1369'
formal_environment['job_API_port'] = '5369'

with open('config.ini', 'w') as configfile:
   config.write(configfile)




# config = configparser.ConfigParser()

# config.read('config.ini')

# config['Test_environment']['database_IP']

# config.sections()

# import configparser
# config = configparser.ConfigParser()
# config['DEFAULT'] = {'ServerAliveInterval': '45',
#                       'Compression': 'yes',
#                       'CompressionLevel': '9'}
# config['bitbucket.org'] = {}
# config['bitbucket.org']['User'] = 'hg'
# config['topsecret.server.com'] = {}
# topsecret = config['topsecret.server.com']
# topsecret['Port'] = '50022'     # mutates the parser
# topsecret['ForwardX11'] = 'no'  # same here
# config['DEFAULT']['ForwardX11'] = 'yes'
