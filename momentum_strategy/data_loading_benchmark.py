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

end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
start_date = (pd.to_datetime(end_date) - relativedelta(years=10)).strftime("%Y-%m-%d")

dates = calendar.schedule(start_date = pd.to_datetime(start_date), end_date = pd.to_datetime(end_date)).index.strftime("%Y-%m-%d").values

benchmark_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
benchmark_data.index = pd.to_datetime(benchmark_data.index, unit="ms", utc=True).tz_convert("America/New_York").strftime("%Y-%m-%d")
benchmark_data = benchmark_data.rename(columns = {"c": "Benchmark Price"})
benchmark_data['Benchmark Return'] = benchmark_data["Benchmark Price"].pct_change().fillna(0) + 1
benchmark_data = benchmark_data.drop(columns = ['v', 'vw', 'o', 'h', 'l', 'n'])
benchmark_data = benchmark_data[benchmark_data.index.isin(dates)]

benchmark_data.to_sql("benchmark_data", con = engine, if_exists = "replace")