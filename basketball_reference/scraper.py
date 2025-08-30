from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time


def init_driver():
    """Initialize headless Chrome driver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def scrape_games(date: str) -> pd.DataFrame:
    """
    Scrape Basketball Reference games table for a given date.
    Args:
        date (str): YYYY-MM-DD
    Returns:
        pd.DataFrame: Games data
    """
    year, month, day = date.split("-")
    url = f"https://www.basketball-reference.com/boxscores/?month={month}&day={day}&year={year}"

    driver = init_driver()
    driver.get(url)
    time.sleep(2)  # wait for JS to render

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    table = soup.find("table", {"id": "schedule"})
    if table:
        df = pd.read_html(str(table))[0]
        return df
    else:
        return pd.DataFrame()  # empty if no games found
