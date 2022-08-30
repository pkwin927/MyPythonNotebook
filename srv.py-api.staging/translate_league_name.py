# -*- coding: utf-8 -*-
"""
Created on Mon Aug  8 10:59:01 2022

@author: JackeyChen陳威宇
"""
import method.log_config as log_config
log_handler,logging = log_config.create_logger('translate_league_name')
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
    league_name = sys.argv[2]
    translate_league_name = sys.argv[3]
    
    # job_name = 'translate_league_name.py'
    # config_number = '2'
    # league_name = 'BSN'
    # translate_league_name = 'BSN_test'
    
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
    
    # aaa = "mysql+pymysql://api2:12345678@192.168.1.239:3306/maindb"
    # engine_betradar = create_engine(aaa,encoding = 'utf8',connect_args={"program_name":"python"})

    # league_DF = pd.read_sql(f"""select * from game_league where LeagueName ="{league_name}" and LanguageCode = "zh-cn" """, con = engine_betradar)
    # league_DF = pd.read_sql(f"""select * from game_match where GameID ="34232235" """, con = engine_betradar)
    league_DF = pd.read_sql(f"""select * from game_league where LeagueName ="{league_name}" and LanguageCode = "zh-cn" """, con = engine_list[1])
    # btd.dispose_engine(engine_betradar)
    # engine_betradar.dispose()
    league_id_list = list(league_DF['LeagueID'].drop_duplicates())
    
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
    for LeagueID in league_id_list:
        
        driver.get(f"https://portal.betradar.com/#/configuration/translation-tool/?n=3&q={LeagueID}")
        time.sleep(5)
            
        league_element = driver.find_element(By.XPATH, f"//p[contains(text(), '{LeagueID}')]")
        
        league_element.click()
        text_area_list = driver.find_elements(By.CSS_SELECTOR,"textarea")
        # text_area_list = driver.find_elements_by_css_selector("textarea") 
        text_area_list[0].click()
        ActionChains(driver).double_click(text_area_list[0]).click(text_area_list[0]).perform()
        text_area_list[0].send_keys(translate_league_name)
        time.sleep(1)
        league_element.click()
        # print('OK')
        time.sleep(1)
        # print('OK')
    
    
    league_DF['LeagueName'] = translate_league_name
    league_DF['Updator'] = config_info['database_user']
    league_DF['UpdateTime'] = pd.to_datetime(dt.datetime.now())
    # league_DF.columns
    btd.update_duplicate_to_sql(league_DF, 'game_league', engine_list[1])
    sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and Job = "translate_league_name.py";"""
    engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    btd.dispose_engine(engine_list)
    driver.quit()
except:
    try:
        driver.quit()
        sql_commend_str = f"""DELETE FROM betradar_API_info WHERE IP = "{local_ip}" and Job = "translate_league_name.py";"""
        engine_list[0].execute(sa_text(sql_commend_str).execution_options(autocommit=True))
    
        btd.dispose_engine(engine_list)
        log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
        log_handler.removeHandler(log_handler.handlers[1])
        logging.shutdown()
        btd.dispose_engine(engine_list)
    except:
        try:
            driver.quit()
            log_handler.exception(f"""#####################{env_name} Catch an exception.#####################""")
            log_handler.removeHandler(log_handler.handlers[1])
            logging.shutdown()
            btd.dispose_engine(engine_list)
        except:
            log_handler.exception(f"""#####################Catch an exception.#####################""")
            log_handler.removeHandler(log_handler.handlers[1])
            logging.shutdown()
            btd.dispose_engine(engine_list)
        
