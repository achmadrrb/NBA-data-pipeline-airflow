"""
An operations workflow to load data from basketball-reference website to BigQuery.
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # stop getting Pandas FutureWarning's

import os
import glob
import time
from datetime import datetime, timedelta
import pandas as pd
from numpy.random import choice
from config import Config 
from google.cloud import bigquery
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup

from modules.api_basketball_reference import clean_data, parse_date, get_box_score_list

# 'False' DAG is ready for operation; i.e., 'True' DAG runs using no BigQuery requests
TESTING_DAG = False
# Minutes to sleep on an error
ERROR_SLEEP_MIN = 5 
# Max number of searches to perform daily
MAX_SEARCHES = 1500
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "airflow"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = ['eririfky29@gmail.com']
START_DATE = airflow.utils.dates.days_ago(1)

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'start_date': START_DATE, 
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0, # removing retries to not call insert duplicates into BigQuery
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'basketball-reference_bigquery', 
    description='Call basketball-reference API and insert results into Bigquery',
    default_args=default_args, 
    schedule_interval='0 13 * * *',
    catchup=False,
    tags=['data-pipeline-dag'],
    max_active_tasks = 3
)

with dag:

    start = DummyOperator(
        task_id='start',
        dag=dag)

    # function for calling api_basketball_reference and insert results into BigQuery
    def _player_daily_results():
        today = parse_date()
        matchday_date = today - timedelta(days=1)

        # Create Null DataFrame for storing combined DataFrame
        combined_df_all = pd.DataFrame()

        basketball_reference_web = "https://www.basketball-reference.com"
        box_score_link = get_box_score_list()
        if len(box_score_link) == 0:
            return

        for box_score in box_score_link:
            box_score_match_url = basketball_reference_web + box_score
            # Initialize Chrome Service with Chromedriver path
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Enable headless mode
            chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
            chrome_options.add_argument("--disable-dev-shm-usage")  # Avoid /dev/shm usage

            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            driver.get(box_score_match_url)
            driver.execute_script("window.scrollTo(1,10000)")
            time.sleep(2)

            page = driver.page_source
            soup = BeautifulSoup(page, 'html.parser')
            team_match_table = soup.find_all("table", attrs="sortable stats_table now_sortable")
            away_index = 0
            home_index = 2
            try:
                away_team_table = pd.read_html(str(team_match_table))[away_index]
                home_team_table = pd.read_html(str(team_match_table))[home_index]
            except IndexError:
                print("IndexError on this html: ", box_score)
                break

            # Using clean_data helper function
            try:
                away_team = clean_data(away_team_table, team_match_table, matchday_date, away_index)
                home_team = clean_data(home_team_table, team_match_table, matchday_date, home_index)
            except ValueError:
                print("ValueError on this html: ", box_score)
                break

            # Combine them
            combined_df_match = pd.concat([away_team, home_team])
            combined_df_match.reset_index(drop=True, inplace=True)

            combined_df_all = pd.concat([combined_df_all, combined_df_match])
            combined_df_all.reset_index(drop=True, inplace=True)

        table_id = Config.table_id
        client = bigquery.Client()
        upload = client.load_table_from_dataframe(combined_df_all, table_id)

        return

    player_daily_results = PythonOperator(
        task_id='player_daily_results',
        provide_context=True,
        python_callable=_player_daily_results
    )
    
    finish = DummyOperator(
        task_id='finish',
        dag=dag)
    
    start >> player_daily_results >> finish


    



