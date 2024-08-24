import requests
import pandas as pd
import numpy as np
import sqlalchemy
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar
from dateutil.relativedelta import relativedelta

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine("sqlite:///C:/Users/lewis/OneDrive/tradingstrategies/databases/momentum_strategy_2_database.db")
tickers = pd.read_sql("SELECT * FROM liquid_stocks", con = engine)["ticker"].values

#12 month look back from previous ME
if datetime.today() != datetime.today().replace(day=1):
    end_date = datetime.today().replace(day=1) - timedelta(days=1)
else:
    end_date = datetime.today() - timedelta(days=1)

start_date = end_date - relativedelta(months=+12) - timedelta(days=1)

dates = calendar.schedule(start_date = start_date, end_date = end_date).index.strftime("%Y-%m-%d").values

stocks_list = pd.DataFrame()
stocks_list['ticker'] = tickers
times = []

#can probably generate returns directly in loop and have only one df but I like having (the code for) both returns and prices
for date in dates:
    
    start_time = datetime.now()
    
    stocks_request = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&apiKey={polygon_api_key}").json()["results"])
    stocks_request = stocks_request[stocks_request["T"].isin(tickers)].copy()
    stocks_request = stocks_request.rename(columns={"T":"ticker"})
    stocks_list[date] = stocks_list['ticker'].map(stocks_request.set_index('ticker')['c'])

    end_time = datetime.now()
    seconds_to_complete = (end_time - start_time).total_seconds()
    times.append(seconds_to_complete)
    iteration = round((np.where(dates==date)[0][0]/len(dates))*100,2)
    iterations_remaining = len(dates) - np.where(dates==date)[0][0]
    average_time_to_complete = np.mean(times)
    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
    time_remaining = estimated_completion_time - datetime.now()
    
    print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

stocks_list.to_sql("stock_prices", con = engine, if_exists = "replace")

stocks_list = stocks_list.set_index('ticker')
stock_returns = stocks_list.pct_change(axis=1)

stock_returns = stock_returns.drop(columns=[stock_returns.columns[0]])
stock_returns.to_sql("stock_returns", con = engine, if_exists = "replace")