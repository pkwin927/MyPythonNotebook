# -*- coding: utf-8 -*-
"""
Created on Mon Aug  8 10:59:01 2022

@author: JackeyChen陳威宇
"""
import method.log_config as log_config
log_handler,logging = log_config.create_logger('translate_team_name')
try:

    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select
    from selenium.webdriver.chrome.options import Options
    from sqlalchemy.sql import text as sa_text
    from webdriver_manager.chrome import ChromeDriverManager
    import time
    import method.betradar_data as btd
    import method.single_API_method as API_method
    import sys
    import pandas as pd
    import datetime as dt
    # TeamID = '711901'
    job_name = sys.argv[0]
    config_number = sys.argv[1]
    team_name = sys.argv[2]
    translate_team_name = sys.argv[3]
    
    # job_name = 'translate_team_name'
    # config_number = '2'
    # team_name = '科里,高芙'
    # translate_team_name = '科里,高芙'
    
    # translate_team_name = 'Gauff, Cori (Srl)'
    # TeamID = 711901
    local_ip = API_method.get_IP()
    engine_list = btd.create_connect(config_number = config_number)
    config_info = API_method.get_config_info(config_number = config_number)
    
    env_name = config_info['env_name']
    backstage_user = config_info['backstage_user']
    backstage_password = config_info['backstage_password']
    control_info = btd.get_control_info(job_name,config_number = config_number)
    control_info.to_sql(name = 'betradar_API_info',con = engine_list[0],if_exists = 'append',index = False)
    
    
    team_DF = pd.read_sql(f"""select * from game_team where TeamName ="{team_name}" and LanguageCode = "zh-cn" """, con = engine_list[1])
    
    team_id_list = list(team_DF['TeamID'].drop_duplicates())
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
    time.sleep(5)
    driver.get('https://portal.betradar.com/#/configuration/translation-tool')
    driver.maximize_window()
    
    time.sleep(3)
    driver.find_element(By.ID,"username").send_keys(f"{backstage_user}")
    driver.find_element(By.ID,"password").send_keys(f"{backstage_password}")
    
    sign_in_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Sign in')]")
    
    # sign_in_button = driver.find_element_by_xpath("//button[contains(text(), 'Sign in')]")
    driver.execute_script('arguments[0].click();',sign_in_button)
    time.sleep(3)
    driver.get('https://portal.betradar.com/#/configuration/translation-tool')
    time.sleep(5)
    # print('stage1')
    for TeamID in team_id_list:
        
        driver.get(f"https://portal.betradar.com/#/configuration/translation-tool/?n=8&q={TeamID}")
        time.sleep(5)
            
        team_element = driver.find_element(By.XPATH, f"//p[contains(text(), '{TeamID}')]")
        
        team_element.click()
        text_area_list = driver.find_elements(By.CSS_SELECTOR,"textarea")
        # text_area_list = driver.find_elements_by_css_selector("textarea") 
        text_area_list[0].click()
        ActionChains(driver).double_click(text_area_list[0]).click(text_area_list[0]).perform()
        text_area_list[0].send_keys(translate_team_name)
        time.sleep(1)
        team_element.click()
        # print('OK')
        time.sleep(1)
        # print('OK')
    
    
    team_DF['TeamName'] = translate_team_name
    team_DF['Updator'] = config_info['database_user']
    team_DF['UpdateTime'] = pd.to_datetime(dt.datetime.now())
    btd.update_duplicate_to_sql(team_DF, 'game_team', engine_list[1])
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and Job = "translate_team_name.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    btd.dispose_engine(engine_list)
    driver.quit()
except:
    try:
        driver.quit()
        sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and Job = "translate_team_name.py";"""
        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    
        btd.dispose_engine(engine_list)
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()
        
    except:
        try:
            driver.quit()
            btd.dispose_engine(engine_list)
            log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
            log_handler.removeHandler(log_handler.handlers[1])
            logging.shutdown()
        except:
            log_handler.exception(f"""#####################Catch an exception.#####################""")
            log_handler.removeHandler(log_handler.handlers[1])
            logging.shutdown()
        
        
