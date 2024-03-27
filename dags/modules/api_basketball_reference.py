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

