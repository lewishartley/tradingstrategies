import requests
import pandas as pd
import numpy as np
import sqlalchemy
import pytz
from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")
date = calendar.schedule(start_date = (datetime.today()-timedelta(days=10)), end_date = datetime.today()).index.strftime("%Y-%m-%d").values[-1]
tz = pytz.timezone("GMT")

all_stocks_1 = requests.get(f"https://api.polygon.io/v3/reference/tickers?type=CS&market=stocks&active=true&order=asc&sort=ticker&limit=1000&apiKey={polygon_api_key}").json()
all_stocks_2 = requests.get(f"{all_stocks_1['next_url']}&apikey={polygon_api_key}").json()
all_stocks_3 = requests.get(f"{all_stocks_2['next_url']}&apikey={polygon_api_key}").json()
all_stocks_4 = requests.get(f"{all_stocks_3['next_url']}&apikey={polygon_api_key}").json()
all_stocks_5 = requests.get(f"{all_stocks_4['next_url']}&apikey={polygon_api_key}").json()
all_stocks_6 = requests.get(f"{all_stocks_5['next_url']}&apikey={polygon_api_key}").json()

stock_list = [all_stocks_1, all_stocks_2, all_stocks_3, all_stocks_4, all_stocks_5, all_stocks_6]
available_stock_list = [] 
total_available_stocks = pd.concat(available_stock_list)
total_available_stocks = pd.DataFrame(total_available_stocks)
engine = sqlalchemy.create_engine("sqlite///C:/Users/lewis/OneDrive/tradingstrategies/databases/momentum_strategy_2_database.db")
total_available_stocks.to_sql("all_stocks", con = engine, if_exists="replace")