import requests
import pandas as pd
import numpy as np
import sqlalchemy
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar
from dateutil.relativedelta import relativedelta

engine = sqlalchemy.create_engine("sqlite:///C:/Users/lewis/OneDrive/tradingstrategies/databases/momentum_strategy_2_database.db")
calendar = get_calendar("NYSE")
polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"

gross_returns = pd.read_sql("SELECT * FROM gross_returns", con = engine)
stock_prices = pd.read_sql("SELECT * FROM stock_returns", con = engine)

gross_returns = gross_returns.set_index('ticker')

end_date = pd.to_datetime(gross_returns.columns[-1]).date()
start_date = end_date - relativedelta(years=3) - relativedelta(months = 11)
dates = calendar.schedule(start_date = start_date, end_date = end_date).index.strftime("%Y-%m-%d").values
dates_shift = calendar.schedule(start_date = start_date + timedelta(days=1), end_date = end_date + timedelta(days=1)).index.strftime("%Y-%m-%d").values

portfolio_returns = pd.DataFrame(columns=['Date', 'Long Momentum Returns', 'Short Momentum Returns', 'Gross Returns'])
times = []

for date in dates:
    start_time = datetime.now()
    if date == dates[0]:
        portfolio_returns = pd.concat([portfolio_returns, pd.DataFrame([{'Date' : date, 'Long Momentum Returns' : 1, 'Short Momentum Returns': 1, 'Gross Returns' : 1}])], ignore_index=True)
    else:
        yday = dates[np.where(dates == date)[0][0]-1]
        screening_df = pd.DataFrame()
        screening_df['ticker'] = gross_returns.index
        screening_df['3m momentum'] = screening_df['ticker'].map(gross_returns.loc[:, (pd.to_datetime(yday).date() - relativedelta(months = 3)).strftime("%Y-%m-%d"):yday].cumprod(axis=1).iloc[:, -1]-1)
        screening_df['1m momentum'] = screening_df['ticker'].map(gross_returns.loc[:, (pd.to_datetime(yday).date() - relativedelta(months = 1)).strftime("%Y-%m-%d"):yday].cumprod(axis=1).iloc[:, -1]-1)
        screening_df['2w momentum'] = screening_df['ticker'].map(gross_returns.loc[:, (pd.to_datetime(yday).date() - relativedelta(weeks = 2)).strftime("%Y-%m-%d"):yday].cumprod(axis=1).iloc[:, -1]-1)
        screening_df['1w momentum'] = screening_df['ticker'].map(gross_returns.loc[:, (pd.to_datetime(yday).date() - relativedelta(weeks = 1)).strftime("%Y-%m-%d"):yday].cumprod(axis=1).iloc[:, -1]-1)
        screening_df['1d momentum'] = screening_df['ticker'].map(gross_returns.loc[:, (pd.to_datetime(yday).date() - relativedelta(days = 1)).strftime("%Y-%m-%d"):yday].cumprod(axis=1).iloc[:, -1]-1)
        #adm = average daily momentum
        screening_df['3m adm'] = screening_df['3m momentum'] / 60
        screening_df['1m adm'] = screening_df['1m momentum'] / 20
        screening_df['2w adm'] = screening_df['2w momentum'] / 10
        screening_df['1w adm'] = screening_df['1w momentum'] / 5
        #ddm = difference in daily momentum
        screening_df['1m3m ddm'] = screening_df['1m adm'] - screening_df['3m adm']
        screening_df['2w1m ddm'] = screening_df['2w adm'] - screening_df['1m adm']
        screening_df['1w2w ddm'] = screening_df['1w adm'] - screening_df['2w adm']
        increasing_momentum_conditions = (screening_df['1w adm'] > 0) & (screening_df['1d momentum'] > screening_df['1w adm']) & (screening_df['1w adm'] > screening_df['2w adm']) & (screening_df['2w adm'] > screening_df['1m adm']) & (screening_df['1m adm'] > screening_df['3m adm']) & (screening_df['1w2w ddm'] > screening_df['2w1m ddm'])# & (screening_df['2w1m ddm'] > screening_df['1m3m ddm'])   
        decreasing_momentum_conditions = ((screening_df['1w adm'] < 0) & screening_df['1w adm'] < screening_df['2w adm']) & (screening_df['2w adm'] < screening_df['1m adm'])# & (screening_df['1m adm'] < screening_df['3m adm']) & (screening_df['1w2w ddm'] < screening_df['2w1m ddm']) & (screening_df['2w1m ddm'] < screening_df['1m3m ddm']) (screening_df['1d momentum'] < screening_df['1w adm']) & 
        long_momentum_portfolio = screening_df[increasing_momentum_conditions]['ticker'].tolist()
        long_momentum_portfolio = screening_df[increasing_momentum_conditions]['ticker'].tolist()
        short_momentum_portfolio = screening_df[decreasing_momentum_conditions]['ticker'].tolist()
        long_return = (gross_returns[gross_returns.index.isin(long_momentum_portfolio)][date]).mean()
        short_return = ((((gross_returns[gross_returns.index.isin(short_momentum_portfolio)][date])-1)*-1)+1).mean()
        denominator = max(len(long_momentum_portfolio)+len(short_momentum_portfolio), 1)
        gross_return = long_return*(len(long_momentum_portfolio)/denominator) + short_return*(len(short_momentum_portfolio)/denominator)
        portfolio_returns = pd.concat([portfolio_returns, pd.DataFrame([{'Date' : date, 'Long Momentum Returns' : long_return, 'Short Momentum Returns': short_return, 'Gross Returns' : gross_return}])], ignore_index=True)
        
        end_time = datetime.now()
        seconds_to_complete = (end_time - start_time).total_seconds()
        times.append(seconds_to_complete)
        iteration = round((np.where(dates==date)[0][0]/len(dates))*100,2)
        iterations_remaining = len(dates) - np.where(dates==date)[0][0]
        average_time_to_complete = np.mean(times)
        estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
        time_remaining = estimated_completion_time - datetime.now()
        
        print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

portfolio_returns = portfolio_returns.set_index('Date')

benchmark_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
benchmark_data.index = pd.to_datetime(benchmark_data.index, unit="ms", utc=True).tz_convert("America/New_York").strftime("%Y-%m-%d")
benchmark_data['Benchmark Return'] = benchmark_data["c"].pct_change().fillna(0) + 1
benchmark_data = benchmark_data.drop(columns = ['v', 'vw', 'o', 'c', 'h', 'l', 'n'])

portfolio_returns = portfolio_returns.join(benchmark_data, how='left')
portfolio_returns['Cumulative Long Returns'] = portfolio_returns['Long Momentum Returns'].cumprod()
portfolio_returns['Cumulative Short Returns'] = portfolio_returns['Short Momentum Returns'].cumprod()
portfolio_returns['Cumulative Gross Returns'] = portfolio_returns['Gross Returns'].cumprod()
portfolio_returns['Cumulative Benchmark Returns'] = portfolio_returns['Benchmark Return'].cumprod()
portfolio_returns['Cumulative L/S Equal Weight Returns'] = ((portfolio_returns['Long Momentum Returns'] + portfolio_returns['Short Momentum Returns'])/2).cumprod()
portfolio_returns.index = pd.to_datetime(portfolio_returns.index)

plt.figure(figsize=(10, 5))
plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Long Returns'], label='Long Momentum Return')
plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Short Returns'], label='Short Momentum Return')
plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Gross Returns'], label='Long/Short Return')
plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Benchmark Returns'], label='Benchmark Return')
plt.plot(portfolio_returns.index, portfolio_returns['Cumulative L/S Equal Weight Returns'], label='L/S Equal Weight Return')

plt.xlabel('Date')
plt.ylabel('Return')
plt.title('Momentum Strategy vs Benchmark Returns')
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.show()