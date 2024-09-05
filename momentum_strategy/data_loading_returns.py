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

end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
start_date = (pd.to_datetime(end_date) - relativedelta(years=10)).strftime("%Y-%m-%d")

dates = calendar.schedule(start_date = pd.to_datetime(start_date), end_date = pd.to_datetime(end_date)).index.strftime("%Y-%m-%d").values

stock_prices = pd.DataFrame(index = dates)
times = []

#can probably generate returns directly in loop and have only one df but I like having (the code for) both returns and prices
for ticker in tickers:
    
    start_time = datetime.now()
    
    stocks_request = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
    stocks_request.index = pd.to_datetime(stocks_request.index, unit="ms", utc=True).tz_convert("America/New_York").strftime("%Y-%m-%d")
    stocks_request = stocks_request.rename(columns={"c": ticker})
    stocks_request = stocks_request.drop(columns = ['v', 'vw', 'o', 'h', 'l', 'n'])
    stock_prices = stock_prices.join([stocks_request], how='left')

    end_time = datetime.now()
    seconds_to_complete = (end_time - start_time).total_seconds()
    times.append(seconds_to_complete)
    iteration = round((np.where(tickers==ticker)[0][0]/len(tickers))*100,2)
    iterations_remaining = len(tickers) - np.where(tickers==ticker)[0][0]
    average_time_to_complete = np.mean(times)
    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
    time_remaining = estimated_completion_time - datetime.now()
    
    print(f"Stock data {iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

stock_returns = stock_prices.pct_change().fillna(0) + 1

stock_prices.to_sql("stock_prices", con = engine, if_exists = "replace")
stock_returns.to_sql("stock_returns", con = engine, if_exists = "replace")