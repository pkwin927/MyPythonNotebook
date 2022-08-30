# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 16:41:41 2022

@author: User
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
from sqlalchemy.sql import text as sa_text
import traceback
import os
import getpass
from apscheduler.schedulers.blocking import BlockingScheduler
import sys
from flask import Flask,jsonify,request
from flask_restx import Resource,Api,reqparse
import socket
from netifaces import interfaces, ifaddresses, AF_INET
import configparser
import redis
import netifaces
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5,AES, PKCS1_OAEP
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Random import get_random_bytes
import base64
import paramiko
import ast
from paramiko import SSHClient
from scp import SCPClient

import method.betradar_data as btd
import method.API_method as API_method



def create_connect(config_number):
    config_info = API_method.get_config_info(config_number = config_number)
    
    database_IP = config_info['database_IP']
    database_port = config_info['database_port']
    database_user = config_info['database_user']
    database_password = config_info['database_password']
    api_database_name = config_info['api_database_name']
    game_database_name = config_info['game_database_name']
    # database_password = "!Q\@W3e4r"
    connect_info_betradar = f"""mysql+pymysql://{database_user}:{database_password}@{database_IP}:{database_port}/{api_database_name}"""
    engine_betradar = create_engine(connect_info_betradar,encoding = 'utf8')

    # connect_info_betradar = 'mysql+pymysql://jackey:1q2w3e4r@18.163.118.239:3306/betradardb'
    # engine_betradar = create_engine(connect_info_betradar,encoding = 'utf8')


    # connect_info = 'mysql+pymysql://jackey:1q2w3e4r@18.163.118.239:3306/maindb'
    # engine_main = create_engine(connect_info,encoding = 'utf8')

    connect_info =  f"""mysql+pymysql://{database_user}:{database_password}@{database_IP}:{database_port}/{game_database_name}"""
    engine_main = create_engine(connect_info,encoding = 'utf8')
    
    return [engine_betradar,engine_main]
    


create_connect("2")