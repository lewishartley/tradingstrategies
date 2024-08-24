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

momentum_dataset = pd.read_sql("SELECT * FROM momentum_dataset", con = engine)
stock_returns = pd.read_sql("SELECT * FROM stock_returns", con = engine)

stock_returns = stock_returns.set_index('ticker')

start_date = pd.to_datetime(stock_returns.columns[0]).date()
end_date = pd.to_datetime(stock_returns.columns[-1]).date()
dates = calendar.schedule(start_date = start_date, end_date = end_date)

days_12m = len(dates)
days_6m = round(len(dates)/2)
days_3m = round(len(dates)/4)
days_1m = round(len(dates)/12)
days_2w = round(len(dates)/24)
days_1w = round(len(dates)/48)

momentum_screening_dataset = pd.DataFrame(momentum_dataset['ticker'])
momentum_dataset = momentum_dataset.set_index('ticker')

momentum_screening_dataset['12m implied 1d momentum'] = momentum_screening_dataset['ticker'].map(momentum_dataset['12m momentum']/days_12m)
momentum_screening_dataset['6m implied 1d momentum'] = momentum_screening_dataset['ticker'].map(momentum_dataset['6m momentum']/days_6m)
momentum_screening_dataset['3m implied 1d momentum'] = momentum_screening_dataset['ticker'].map(momentum_dataset['3m momentum']/days_3m)
momentum_screening_dataset['1m implied 1d momentum'] = momentum_screening_dataset['ticker'].map(momentum_dataset['1m momentum']/days_1m)
momentum_screening_dataset['2w implied 1d momentum'] = momentum_screening_dataset['ticker'].map(momentum_dataset['2w momentum']/days_2w)
momentum_screening_dataset['1w implied 1d momentum'] = momentum_screening_dataset['ticker'].map(momentum_dataset['1w momentum']/days_1w)

momentum_screening_dataset['1d momentum'] = momentum_screening_dataset['ticker'].map(momentum_dataset['1d momentum'])

momentum_screening_dataset['12m6m difference'] = momentum_screening_dataset['6m implied 1d momentum'] - momentum_screening_dataset['12m implied 1d momentum']
momentum_screening_dataset['6m3m difference'] = momentum_screening_dataset['3m implied 1d momentum'] - momentum_screening_dataset['6m implied 1d momentum']
momentum_screening_dataset['3m1m difference'] = momentum_screening_dataset['1m implied 1d momentum'] - momentum_screening_dataset['3m implied 1d momentum']
momentum_screening_dataset['1m2w difference'] = momentum_screening_dataset['2w implied 1d momentum'] - momentum_screening_dataset['1m implied 1d momentum']
momentum_screening_dataset['2w1w difference'] = momentum_screening_dataset['1w implied 1d momentum'] - momentum_screening_dataset['2w implied 1d momentum']
momentum_screening_dataset['1w1d difference'] = momentum_screening_dataset['1d momentum'] - momentum_screening_dataset['1w implied 1d momentum']

increasing_momentum_conditions = (momentum_screening_dataset['1w implied 1d momentum'] > 0) & (momentum_screening_dataset['2w1w difference'] > momentum_screening_dataset['1m2w difference']) & (momentum_screening_dataset['1w implied 1d momentum'] > momentum_screening_dataset['2w implied 1d momentum']) & (momentum_screening_dataset['2w implied 1d momentum'] > momentum_screening_dataset['1m implied 1d momentum'])
increasing_momentum = momentum_screening_dataset[increasing_momentum_conditions]['ticker'].reset_index()

momentum_screening_dataset.to_sql("momentum_screening_dataset", con = engine, if_exists="replace")
increasing_momentum.to_sql("increasing_momentum", con = engine, if_exists="replace")