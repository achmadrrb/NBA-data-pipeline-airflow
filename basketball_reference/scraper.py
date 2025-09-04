from io import StringIO
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


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()  # remove leading/trailing spaces
        .str.replace(r"%", "_percent", regex=True)  # replace % with _percent
        .str.replace(r"[^\w]", "_", regex=True)  # replace non-alphanumeric with _
        .str.replace(r"__+", "_", regex=True)  # collapse multiple underscores
        .str.lower()  # optional: lowercase for consistency
    )
    return df


def scrape_games(date: str) -> pd.DataFrame:
    """
    Scrape Basketball Reference games table for a given date.
    Args:
        date (str): YYYY-MM-DD
    Returns:
        pd.DataFrame: Games data
    """
    year, month, day = date.split("-")
    url = f"https://www.basketball-reference.com/friv/dailyleaders.fcgi?month={month}&day={day}&year={year}"

    driver = init_driver()
    driver.get(url)
    time.sleep(2)  # wait for JS to render

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    table = soup.find_all(
        "table", attrs="sortable stats_table now_sortable sticky_table eq2 re2 le2"
    )
    if table:
        table_html = str(table[0])
        df = pd.read_html(StringIO(table_html))[0]
        df = clean_column_names(df)
        return df
    else:
        return pd.DataFrame()  # empty if no games found
