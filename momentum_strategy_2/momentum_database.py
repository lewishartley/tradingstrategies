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

tickers = pd.read_sql("SELECT * FROM all_stocks", con = engine)["ticker"].values
stock_returns = pd.read_sql("SELECT * FROM stock_returns", con = engine)

momentum_dataset = pd.DataFrame(stock_returns['ticker'])

stock_returns = stock_returns.set_index('ticker')

gross_returns = stock_returns + 1
gross_returns.to_sql("gross_returns", con = engine, if_exists="replace")

#one thing that is a bit dodgy here is if there are market holidays in when any of the relative changes fall. Will look into using nyse.schedule() somehow
momentum_dataset['12m momentum'] = momentum_dataset['ticker'].map(gross_returns.loc[:, (pd.to_datetime(gross_returns.columns[-1]).date() - relativedelta(months = 12)).strftime("%Y-%m-%d"):].cumprod(axis=1).iloc[:, -1]-1)
momentum_dataset['6m momentum'] = momentum_dataset['ticker'].map(gross_returns.loc[:, (pd.to_datetime(gross_returns.columns[-1]).date() - relativedelta(months = 6)).strftime("%Y-%m-%d"):].cumprod(axis=1).iloc[:, -1]-1)
momentum_dataset['3m momentum'] = momentum_dataset['ticker'].map(gross_returns.loc[:, (pd.to_datetime(gross_returns.columns[-1]).date() - relativedelta(months = 3)).strftime("%Y-%m-%d"):].cumprod(axis=1).iloc[:, -1]-1)
momentum_dataset['1m momentum'] = momentum_dataset['ticker'].map(gross_returns.loc[:, (pd.to_datetime(gross_returns.columns[-1]).date() - relativedelta(months = 1)).strftime("%Y-%m-%d"):].cumprod(axis=1).iloc[:, -1]-1)
momentum_dataset['2w momentum'] = momentum_dataset['ticker'].map(gross_returns.loc[:, (pd.to_datetime(gross_returns.columns[-1]).date() - relativedelta(weeks = 2)).strftime("%Y-%m-%d"):].cumprod(axis=1).iloc[:, -1]-1)
momentum_dataset['1w momentum'] = momentum_dataset['ticker'].map(gross_returns.loc[:, (pd.to_datetime(gross_returns.columns[-1]).date() - relativedelta(weeks = 1)).strftime("%Y-%m-%d"):].cumprod(axis=1).iloc[:, -1]-1)
momentum_dataset['1d momentum'] = momentum_dataset['ticker'].map(gross_returns.loc[:, gross_returns.columns[-2]:].cumprod(axis=1).iloc[:, -1]-1)

momentum_dataset.to_sql("momentum_dataset", con = engine, if_exists="replace")
