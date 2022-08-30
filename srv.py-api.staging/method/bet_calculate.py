# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 09:13:47 2022

@author: JackeyChen陳威宇
"""

import math

def odds_change(odds, odds_type = 'EU', change_type = 'HK'):
    type_list = ['EU','HK','MY','ID','US']
    if odds_type not in type_list:
        raise ValueError('odds_type just support '+str(type_list))
        
    if change_type not in type_list:
        raise ValueError('odds_type just support '+str(type_list)) 
        
    if odds_type != 'HK':
        if odds_type == 'EU':
            HK_odds = odds - 1
        elif odds_type == 'MY':
            if odds > 0:
                HK_odds = odds
            else:
                HK_odds = -1 / odds
        elif odds_type == 'ID':
            if odds > 0:
                HK_odds = odds
            else:
                HK_odds = -1 / odds
        elif odds_type == 'US':
            if odds > 0:
                HK_odds = odds / 100
                
            else:
                HK_odds = -100 / odds
                
    else:
        HK_odds = odds
        
    HK_odds = round(HK_odds,2)
    
    if change_type == 'EU':
        result = HK_odds + 1
        result = round(result,2)
    elif change_type == 'HK':
        result = HK_odds
        result = round(result,2)
    elif change_type == 'MY':
        if HK_odds <= 1:
            result = HK_odds
        else:
            result = -1 / HK_odds
        result = round(result,2)
    elif change_type == 'ID':
        if HK_odds >= 1:
            result = HK_odds
            
        else:
            result = -1 / HK_odds
            
        result = round(result,2)
    elif change_type == 'US':
        if HK_odds >= 1:
            result = 100 * HK_odds
        else:
            result = -100 / HK_odds
        result = math.ceil(result)
    return result



