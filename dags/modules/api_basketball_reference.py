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