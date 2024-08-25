import requests
import pandas as pd
import numpy as np
import sqlalchemy
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine("sqlite:///C:/Users/lewis/OneDrive/tradingstrategies/databases/momentum_strategy_2_database.db")
tickers = pd.read_sql("SELECT * FROM all_stocks", con = engine)["ticker"].values

end_date = (datetime.today()-timedelta(days=1))
start_date = (end_date-timedelta(weeks=2))
dates = calendar.schedule(start_date = start_date, end_date = end_date).index.strftime("%Y-%m-%d").values

stocks_list = []
times = []

for date in dates:
    
    start_time = datetime.now()
    
    stocks_request = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true&apiKey={polygon_api_key}").json()["results"])
    stocks_request = stocks_request[stocks_request["T"].isin(tickers)].copy()
    stocks_list.append(stocks_request)
    
    end_time = datetime.now()
    seconds_to_complete = (end_time - start_time).total_seconds()
    times.append(seconds_to_complete)
    iteration = round((np.where(dates==date)[0][0]/len(dates))*100,2)
    iterations_remaining = len(dates) - np.where(dates==date)[0][0]
    average_time_to_complete = np.mean(times)
    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
    time_remaining = estimated_completion_time - datetime.now()
    
    print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

stocks_notional = pd.concat(stocks_list).dropna()
stocks_notional['notional'] = stocks_notional['c'] * stocks_notional['v']
stocks_notional = stocks_notional.drop(columns=['v','vw','o','c','h','l','t','n'])
stocks_notional = stocks_notional.groupby('T', as_index=False).sum()
stocks_notional = stocks_notional.sort_values('notional', ascending=False)
stocks_notional = stocks_notional.head(500)
stocks_notional = stocks_notional.rename(columns={'T': 'ticker'})
stocks_notional = stocks_notional.reset_index(drop = True)

stocks_notional.to_sql("liquid_stocks", con = engine, if_exists = "replace")