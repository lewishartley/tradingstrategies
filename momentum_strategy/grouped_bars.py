import requests
import pandas as pd
import numpy as np
import sqlalchemy
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine("sqlite:///C:/Users/lewis/OneDrive/tradingstrategies/databases/momentum_strategy_database.db")
tickers = pd.read_sql("SELECT * FROM all_stocks", con = engine)["ticker"].values

dates = calendar.schedule(start_date = "2022-01-01", end_date = (datetime.today()-timedelta(days=1))).index.strftime("%Y-%m-%d").values

grouped_bars_list = []
times = []

for date in dates:
    
    start_time = datetime.now()
    
    grouped_bars_request = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&apiKey={polygon_api_key}").json()["results"])
    grouped_bars_request = grouped_bars_request[grouped_bars_request["T"].isin(tickers)].copy()
    grouped_bars_list.append(grouped_bars_request)
    
    end_time = datetime.now()
    seconds_to_complete = (end_time - start_time).total_seconds()
    times.append(seconds_to_complete)
    iteration = round((np.where(dates==date)[0][0]/len(dates))*100,2)
    iterations_remaining = len(dates) - np.where(dates==date)[0][0]
    average_time_to_complete = np.mean(times)
    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
    time_remaining = estimated_completion_time - datetime.now()
    
    print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")
    
full_grouped_bars = pd.concat(grouped_bars_list).dropna()
full_grouped_bars["t"] = pd.to_datetime(full_grouped_bars["t"], unit = "ms")
full_grouped_bars = full_grouped_bars.set_index("t")

full_grouped_bars["pct_change"] = round(full_grouped_bars.groupby("T")["c"].pct_change() * 100, 2)
full_grouped_bars = full_grouped_bars.rename(columns={"T":"ticker"})

full_grouped_bars.to_sql("grouped_bars", con = engine, if_exists = "replace")