# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 15:20:46 2022

@author: JackeyChen陳威宇
"""

import os
import sys

config_number = sys.argv[1]

os.system(f'python update_sports_data.py {config_number}') 

os.system(f'python betradar_to_maindb.py {config_number}')