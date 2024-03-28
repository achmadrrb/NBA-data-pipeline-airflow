"""
An API to collect data from Basketball Reference website

Basketball Reference website: https://www.basketball-reference.com/
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning) # stop getting Pandas FutureWarning's

from selenium import webdriver
import time
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import pytz

def _parse_date_now(tzone='US/Eastern'):
    """
    Parse default date now in Eastern Time. Basketball reference website is using US/Eastern timezone

    :param tzone: target timezone. The default is 'US/Eastern'.
    :return: a date as now in Eastern Time.
    """

    # Create a timezone-aware object for UTC
    utc_timezone = pytz.utc

    # Get the current date and time in UTC using timezone-aware objects
    utc_now = datetime.now(tz=utc_timezone)

    # Specify the target timezone as Eastern Time Zone (ET)
    target_timezone = pytz.timezone(tzone)

    # Convert the UTC date to the Eastern Time Zone
    date_now = utc_now.astimezone(target_timezone).date()

    return date_now

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

def _get_box_score_list():
    """
    Get a box score list in a match day.

    :return: A list of html address of box score in a match day
    """

    # Convert the UTC date to the Eastern Time Zone
    date_now = _parse_date_now()

    # Convert the Eastern Time Zone to month, day, and year
    month_now, day_now, year_now = _convert_date_to_str(date_now)

    # Combine month, day and year to the url
    day_match_url = "https://www.basketball-reference.com/boxscores/index.fcgi?month={month}&day={day}&year={year}"
    url = day_match_url.format(month=month_now, day=day_now, year=year_now)

    # Get the page using Chrome WebDriver to get html elements
    driver = webdriver.Chrome()
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

def _add_team_name_col(df, team_index):
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