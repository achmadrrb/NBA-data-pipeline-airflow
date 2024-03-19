"""
An API to collect data from Basketball Reference website

Basketball Reference website: https://www.basketball-reference.com/
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # stop getting Pandas FutureWarning's

import os
import shutil
from selenium import webdriver
import time
from bs4 import BeautifulSoup
import pandas as pd

driver = webdriver.Chrome()

# Player stats
def player_stats(year):
    player_stats_url = "https://www.basketball-reference.com/leagues/NBA_{}_per_game.html"

    url = player_stats_url.format(year)
    
    driver = webdriver.Chrome()
    driver.get(url)
    driver.execute_script("window.scrollTo(1,10000)")
    time.sleep(2)

    dfs = []
    page = driver.page_source
    soup = BeautifulSoup(page, 'html.parser')
    soup.find('tr', class_="thead").decompose()
    player_table = soup.find_all(id="per_game_stats")[0]
    player_df = pd.read_html(str(player_table))[0]
    player_df["Year"] = year
    dfs.append(player_df)

    players = pd.concat(dfs)

    return players