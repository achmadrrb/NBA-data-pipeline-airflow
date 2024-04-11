"""
An API to collect data from Basketball Reference website

Basketball Reference website: https://www.basketball-reference.com/
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # stop getting Pandas FutureWarning's

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import pytz

def parse_date(date_previous=None):
    """
    Parse default date now in Eastern Time. Basketball reference website is using US/Eastern timezone

    :param date_previous: target date. This paramater is for specifying previous match date that want to be parsed.
                          The match date is in Eastern Time (based on NBA schedule). Format date (str) : YYYYMMDD or YYYY-MM-DD
    :return: a date as now in Eastern Time.
    """

    if date_previous == None:
        # Create a timezone-aware object for UTC
        utc_timezone = pytz.utc

        # Get the current date and time in UTC using timezone-aware objects
        utc_now = datetime.now(tz=utc_timezone)

        # Specify the target timezone as Eastern Time Zone (ET)
        target_timezone = pytz.timezone('US/Eastern')

        # Convert the UTC date to the Eastern Time Zone
        match_date = utc_now.astimezone(target_timezone).date()
    else: 
        match_date = date.fromisoformat(date_previous)

    return match_date

def _convert_date_to_str(date_now):
    """
    Convert date to string; month, day, and year

    :param date_now: a date as now.
    :return: month, day, and year as US format MMDDYYYY
    """
    # Convert the Eastern Time Zone to month, day, and year
    month_now = date_now.strftime("%m")
    if month_now[0] == "0":
        month_now = month_now[1]
    day_now = date_now.strftime("%d")
    year_now = date_now.strftime("%Y")

    return month_now, day_now, year_now

def get_box_score_list(date_previous=None):
    """
    Get a box score list in a match day.

    :param date_previous: target date. This paramater is for specifying previous match date that want to be parsed.
                          The match date is in Eastern Time (based on NBA schedule). Format date (str) : YYYYMMDD or YYYY-MM-DD
    :return: A list of html address of box score in a match day
    """

    # Convert the UTC date to the Eastern Time Zone
    date_now = parse_date(date_previous)

    # Convert the Eastern Time Zone to month, day, and year
    month_now, day_now, year_now = _convert_date_to_str(date_now)

    # Combine month, day and year to the url
    day_match_url = "https://www.basketball-reference.com/boxscores/index.fcgi?month={month}&day={day}&year={year}"
    url = day_match_url.format(month=month_now, day=day_now, year=year_now)

    # Get the page using Chrome WebDriver to get html elements
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Enable headless mode
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
    chrome_options.add_argument("--disable-dev-shm-usage")  # Avoid /dev/shm usage
    # Path to the Chrome WebDriver executable
    chrome_driver_path = '/usr/lib/chromium-browser/chromedriver'
    service = Service(executable_path=chrome_driver_path)

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)
    driver.execute_script("window.scrollTo(1,10000)")
    time.sleep(2)
    page = driver.page_source

    # Using BeautifulSoup to parse the html of each match i.e. '/boxscores/202403180BOS.html'
    soup = BeautifulSoup(page, 'html.parser')
    box_score_link = soup.find_all("a", string="Box Score")
    box_score_list = []
    for link in box_score_link:
        box_score_list.append(link.get('href'))

    return box_score_list

def _proper_table(df):
    """
    Drop unnecessary multi-index, rows, and change column name Starters to Player

    :param df: dataframe that want to be transformed with dropping unnecessarry multi-index, rows, and changing column name 'Starters' to 'Player'
    :return: A transformed dataframe
    """
    # drop Basic Box Score Stats index
    df = df.droplevel(0, axis=1) 
    # rename the Starters column to Player column
    df.rename(columns = {'Starters':'Player'}, inplace = True)
    # you can use a method to drop Reserves row and Team Totals row instead of split
    df.drop(len(df)-1, axis=0, inplace=True)
    df.drop(5, inplace=True)
    df.reset_index(drop=True, inplace=True)
    # drop the DNP players
    df.drop(df[df['MP'] == 'Did Not Play'].index, inplace=True)
    df.drop(df[df['MP'] == 'Did Not Dress'].index, inplace=True)
    df.drop(df[df['MP'] == 'Not With Team'].index, inplace=True)

    return df

def _convert_dtypes(df):
    """
    convert column data types to the right data types

    :param df: dataframe that want to be converted
    :return: A right converted data types dataframe
    """
    # extract MP column to the right value
    minute_played = df["MP"].apply(lambda x: int(x.split(':')[0]))
    second_played = df["MP"].apply(lambda x: int(x.split(':')[1]))
    df["MP"] = minute_played + round(second_played/60, 1)
    # convert each column to corresponding data types
    convert_dict = {'FG': int,
                    'FGA': int,
                    'FG%': float,
                    '3P': int,
                    '3PA': int,
                    '3P%': float,
                    'FT': int,
                    'FTA': int,
                    'FT%': float,
                    'ORB': int,
                    'DRB': int,
                    'TRB': int,
                    'AST': int,
                    'STL': int,
                    'BLK': int,
                    'TOV': int,
                    'PF': int,
                    'PTS': int,
                    }
    df = df.astype(convert_dict)

    # multiply % columns by 100 to get right value
    df["FG%"] = df["FG%"] * 100
    df["3P%"] = df["3P%"] * 100
    df["FT%"] = df["FT%"] * 100

    return df

def _add_match_date_col(df, date_now):
    """
    Add match day date to dataframe

    :param df: dataframe that want to be added
    :return: A dataframe with match day date in it
    """
    new_matchdate_col = date_now
    loc_player = df.columns.get_loc('Player')
    try:
        df.insert(loc=loc_player + 1, column='Date', value=new_matchdate_col)
    except ValueError:
        df["Date"] = new_matchdate_col

    return df

def _add_team_name_col(df, team_match_table, team_index):
    """
    Add team name to dataframe

    :param df: dataframe that want to be added
    :param team_index: a team index to get a team name
    :return: A dataframe with team name in it
    """
    ## add team name
    team_caption = team_match_table[team_index].find_all('caption')[0]
    team_name = team_caption.string
    new_team_col = team_name.split('Basic')[0].strip()
    loc_date = df.columns.get_loc('Date')
    try:
        df.insert(loc=loc_date + 1, column='Tm', value=new_team_col)
    except ValueError:
        df['Tm'] = new_team_col

    return df

def _add_lineup_pos(df):
    """
    Add lineup position to dataframe to determine whether the player is a starter or not

    :param df: dataframe that want to be added
    :return: A dataframe with Game and Game Started in it
    """
    ## add lineup_pos
    loc_team = df.columns.get_loc('Tm')
    try:
        df.insert(loc=loc_team + 1, column='G', value=1)
        df.insert(loc=loc_team + 2, column='GS', value=1)
    except ValueError:
        df['G'] = 1
        df['GS'] = 1
    df.loc[5:, 'GS'] = 0

    return df

def _add_2s_col(df):
    """
    Add 2-point-made(2PM) 2-point-attempt(2PA) and 2-point-percentage(2P%) to dataframe

    :param df: dataframe that want to be added
    :return: A dataframe with 2PM 2PA and 2P% in it
    """
    ## add 2P 2PA and 2P%
    new_2P_col = df["FG"] - df["3P"]
    new_2PA_col = df["FGA"] - df["3PA"]
    new_2PP_col = round(((new_2P_col/new_2PA_col) * 100), 1)
    loc_3P = df.columns.get_loc('3P%')
    try:
        df.insert(loc=loc_3P + 1, column='2P', value=new_2P_col)
        df.insert(loc=loc_3P + 2, column='2PA', value=new_2PA_col)
        df.insert(loc=loc_3P + 3, column='2P%', value=new_2PP_col)
    except ValueError:
        df["2P"] = new_2P_col
        df["2PA"] = new_2PA_col
        df["2P%"] = new_2PP_col
    
    return df

def _add_eFG_col(df):
    """
    Add effective field goals percentage (eFG%) to dataframe

    :param df: dataframe that want to be added
    :return: A dataframe with eFG% in it
    """
    ## add eFG%
    new_eFG_col = round(((df["FG"] + (0.5 * df["3P"]))/df["FGA"]) * 100, 1)
    loc_2P = df.columns.get_loc('2P%')
    try:
        df.insert(loc=loc_2P + 1, column='eFG%', value=new_eFG_col)
    except ValueError:
        df["eFG%"] = new_eFG_col
        
    return df

def _extract_PM_col(df):
    """
    Extract "+/-" column into seperate columns PLUS and MINUS to dataframe

    :param df: dataframe that want to be added
    :return: A dataframe with PLUS and MINUS column in it
    """
    # extract +/- column to separate column named PLUS and MINUS then drop the original column (+/- column)
    loc_PF = df.columns.get_loc('PF')
    new_PLUS_col = df["+/-"].apply(lambda x: int(x[1:]) if x[0] == "+" else 0)
    new_MINUS_col = df["+/-"].apply(lambda x: int(x[1:]) if x[0] == "-" else 0)
    try:
        df.insert(loc=loc_PF + 1, column='PLUS', value=new_PLUS_col)
        df.insert(loc=loc_PF + 2, column='MINUS', value=new_MINUS_col)
    except ValueError:
        df["PLUS"] = df["+/-"].apply(lambda x: int(x[1:]) if x[0] == "+" else 0)
        df["MINUS"]  = df["+/-"].apply(lambda x: int(x[1:]) if x[0] == "-" else 0)
    df = df.drop(columns=['+/-'])

    return df

def clean_data(df, team_match_table, date_now, team_index):
    """
    A combination of function that to clean data in dataframe

    :param df: dataframe that want to be cleaned
    :param datenow: a match day date in Eastern Time
    :param team_index: a team index to get a team name
    :return: A cleaned dataframe
    """
    # data cleaning helper function
    df_proper_table = _proper_table(df)

    # Data cleaning home_starters
    # extract MP column to the right value
    df_right_dtypes = _convert_dtypes(df_proper_table)

    # Add supporting columns
    ## add match day date
    df_date_col = _add_match_date_col(df_right_dtypes, date_now)

    ## add team name
    df_team_col = _add_team_name_col(df_date_col, team_match_table, team_index)

    ## add lineup_pos
    df_lineup_pos_col  = _add_lineup_pos(df_team_col)

    ## add 2P 2PA and 2P%
    df_2s_col = _add_2s_col(df_lineup_pos_col)

    ## add eFG%
    df_eFG_col = _add_eFG_col(df_2s_col)

    ## extract +/- column to separate column named PLUS and MINUS then drop the original column (+/- column)
    df_PM_col = _extract_PM_col(df_eFG_col)

    # fill NaN with 0.0
    df = df_PM_col.fillna(0.0)

    return df