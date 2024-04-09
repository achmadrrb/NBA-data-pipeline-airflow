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
    # schedule='0 12 * * *',
    schedule_interval='@once',
    catchup=False,
    tags=['data-pipeline-dag'],
    max_active_tasks = 3
)

with dag:

    start = DummyOperator(
        task_id='start',
        dag=dag)

    # function for scraping box score html using box_score_link and storing in local dags/modules/boxscore
    def _box_score_html():

        basketball_reference_web = "https://www.basketball-reference.com"
        box_score_link = get_box_score_list(date_previous='2024-04-03')

        folder_path = '/modules/boxscore'

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        for box_score in box_score_link:
            box_score_match_url = basketball_reference_web + box_score
            driver = webdriver.Chrome()
            driver.get(box_score_match_url)
            driver.execute_script("window.scrollTo(1,10000)")
            time.sleep(2)

            page = driver.page_source
            soup = BeautifulSoup(page, 'html.parser')

            with open("/boxscore/{}".format(box_score.split('/')[2]), "w") as file:
                file.write(str(soup))

        return

    box_score_html = PythonOperator(
        task_id='box_score_list',
        provide_context=True,
        python_callable=_box_score_html
    )

    # function for calling api_basketball_reference and insert results into BigQuery
    def _player_daily_results():
        date_now = parse_date(date_previous='2024-04-03')

        # Create Null DataFrame for storing combined DataFrame
        combined_df_all = pd.DataFrame()

        folder_path = '/modules/boxscore'
        for filename in glob.glob(os.path.join(folder_path, '*.html')):
            with open(filename, 'r') as file:
                # Process the file here
                 page = file.read()

            soup = BeautifulSoup(page, 'html.parser')
            team_match_table = soup.find_all("table", attrs="sortable stats_table now_sortable")
            away_index = 0
            home_index = 2
            try:
                away_team_table = pd.read_html(str(team_match_table))[away_index]
                home_team_table = pd.read_html(str(team_match_table))[home_index]
            except IndexError:
                print("IndexError on this html: ", filename)
                break

            # Using clean_data helper function
            try:
                away_team = clean_data(away_team_table, team_match_table, date_now, away_index)
                home_team = clean_data(home_team_table, team_match_table, date_now, home_index)
            except ValueError:
                print("ValueError on this html: ", filename)
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
    
    start >> box_score_html >> player_daily_results >> finish


    



