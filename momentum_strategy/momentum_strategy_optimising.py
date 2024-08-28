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

stock_returns = pd.read_sql("SELECT * FROM stock_returns", con = engine)
stock_returns = stock_returns.set_index('ticker')
gross_returns = stock_returns + 1

times = []
#portfolios = pd.DataFrame(columns=['Date', 'Long Portfolio', 'Short Portfolio'])
summary_stats = pd.DataFrame(columns=['Long Return', 'Short Return', 'L/S Return', 'Benchmark Return' 'Long Std', 'Short Std', 'L/S Std', 'Benchmark Std', 'L/S Benchmark Corr'])

for i in range(1, 30):
    start_time = datetime.now()
    portfolio_returns = pd.DataFrame(columns=['Date', 'Long Momentum Returns', 'Short Momentum Returns', 'Gross Returns'])
    start_date_long = (pd.to_datetime(gross_returns.columns[0]).date())
    end_date = pd.to_datetime(gross_returns.columns[-1]).date()
    dates_long = calendar.schedule(start_date = start_date_long, end_date = end_date).index.strftime("%Y-%m-%d").values
    start_date_short = pd.to_datetime(dates_long[i*3]).date()
    dates_short = calendar.schedule(start_date = start_date_short, end_date = end_date).index.strftime("%Y-%m-%d").values

    for date in dates_short:
        if date == dates_short[0]:
            portfolio_returns = pd.concat([portfolio_returns, pd.DataFrame([{'Date' : date, 'Long Momentum Returns' : 1, 'Short Momentum Returns': 1, 'Gross Returns' : 1}])], ignore_index=True)
        else:
            yday = dates_short[np.where(dates_short == date)[0][0]-1]
            screening_df = pd.DataFrame()
            screening_df['ticker'] = gross_returns.index
            screening_df['t momentum'] = screening_df['ticker'].map(gross_returns.loc[:, dates_long[np.where(dates_long == yday)[0][0]-i]:yday].cumprod(axis=1).iloc[:, -1]-1)
            screening_df['t-1 momentum'] = screening_df['ticker'].map(gross_returns.loc[:, dates_long[np.where(dates_long == yday)[0][0]-i*2]:dates_long[np.where(dates_long == yday)[0][0]-i]].cumprod(axis=1).iloc[:, -1]-1)
            screening_df['t-2 momentum'] = screening_df['ticker'].map(gross_returns.loc[:, dates_long[np.where(dates_long == yday)[0][0]-i*3]:dates_long[np.where(dates_long == yday)[0][0]-i*2]].cumprod(axis=1).iloc[:, -1]-1)
            #adm = average daily momentum
            screening_df['t adm'] = screening_df['t momentum'] / i
            screening_df['t-1 adm'] = screening_df['t-1 momentum'] / i
            screening_df['t-2 adm'] = screening_df['t-2 momentum'] / i
            #ddm = difference in daily momentum
            screening_df['t/t-1 ddm'] = screening_df['t adm'] - screening_df['t-1 adm']
            screening_df['t-1/t-2 ddm'] = screening_df['t-1 adm'] - screening_df['t-2 adm']
            #I am trying to play around with the conditions here to see what maximises returns. Not sure the best way of optimising
            increasing_momentum_conditions = (screening_df['t adm'] > screening_df['t-1 adm']) & (screening_df['t-1 adm'] > screening_df['t-2 adm']) & (screening_df['t/t-1 ddm'] > screening_df['t-1/t-2 ddm'])
            decreasing_momentum_conditions = (screening_df['t adm'] < screening_df['t-1 adm']) & (screening_df['t-1 adm'] < screening_df['t-2 adm']) & (screening_df['t/t-1 ddm'] < screening_df['t-1/t-2 ddm'])
            long_momentum_portfolio = screening_df[increasing_momentum_conditions]['ticker'].tolist()
            short_momentum_portfolio = screening_df[decreasing_momentum_conditions]['ticker'].tolist()
            #portfolios = pd.concat([portfolios, pd.DataFrame([{'Date' : date, 'Long Portfolio' : long_momentum_portfolio, 'Short Portfolio' : short_momentum_portfolio}])], ignore_index=True)
            long_return = (gross_returns[gross_returns.index.isin(long_momentum_portfolio)][date]).mean()
            short_return = ((((gross_returns[gross_returns.index.isin(short_momentum_portfolio)][date])-1)*-1)+1).mean()
            denominator = max(len(long_momentum_portfolio)+len(short_momentum_portfolio), 1)
            gross_return = long_return*(len(long_momentum_portfolio)/denominator) + short_return*(len(short_momentum_portfolio)/denominator)
            portfolio_returns = pd.concat([portfolio_returns, pd.DataFrame([{'Date' : date, 'Long Momentum Returns' : long_return, 'Short Momentum Returns': short_return, 'Gross Returns' : gross_return}])], ignore_index=True)

    portfolio_returns = portfolio_returns.set_index('Date')

    benchmark_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start_date_short}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
    benchmark_data.index = pd.to_datetime(benchmark_data.index, unit="ms", utc=True).tz_convert("America/New_York").strftime("%Y-%m-%d")
    benchmark_data['Benchmark Return'] = benchmark_data["c"].pct_change().fillna(0) + 1
    benchmark_data = benchmark_data.drop(columns = ['v', 'vw', 'o', 'c', 'h', 'l', 'n'])
    portfolio_returns = portfolio_returns.join(benchmark_data, how='left')
    
    portfolio_returns['Cumulative Long Returns'] = portfolio_returns['Long Momentum Returns'].cumprod()
    portfolio_returns['Cumulative Short Returns'] = portfolio_returns['Short Momentum Returns'].cumprod()
    portfolio_returns['Cumulative Gross Returns'] = portfolio_returns['Gross Returns'].cumprod()
    portfolio_returns['Cumulative Benchmark Returns'] = portfolio_returns['Benchmark Return'].cumprod()
    portfolio_returns['Equal-weighted L/S Returns'] = (portfolio_returns['Long Momentum Returns']+portfolio_returns['Short Momentum Returns'])/2
    portfolio_returns['Cumulative Equal-weighted L/S Returns'] = portfolio_returns['Equal-weighted L/S Returns'].cumprod()
    portfolio_returns.index = pd.to_datetime(portfolio_returns.index)

    summary_stats_temp = pd.DataFrame({'Long Return' : round((portfolio_returns['Cumulative Long Returns'].iloc[-1]-1)*100,2),
                                    'Short Return' : round((portfolio_returns['Cumulative Short Returns'].iloc[-1]-1)*100,2),
                                    'L/S Return' : round((portfolio_returns['Cumulative Gross Returns'].iloc[-1]-1)*100,2),
                                    'Benchmark Return' : round((portfolio_returns['Cumulative Benchmark Returns'].iloc[-1]-1)*100,2),
                                    'Long Std' : round(portfolio_returns['Long Momentum Returns'].std(),4),
                                    'Short Std' : round(portfolio_returns['Short Momentum Returns'].std(),4),
                                    'L/S Std' : round(portfolio_returns['Gross Returns'].std(),4),
                                    'Benchmark Std' : round(portfolio_returns['Benchmark Return'].std(),4),
                                    'L/S Benchmark Corr' : round(portfolio_returns['Gross Returns'].corr(portfolio_returns['Benchmark Return']),4)}, index = [i])
    summary_stats = pd.concat([summary_stats, summary_stats_temp], ignore_index=False)

    end_time = datetime.now()
    seconds_to_complete = (end_time - start_time).total_seconds()
    times.append(seconds_to_complete)
    iteration = round((i/30)*100,2)
    iterations_remaining = 30 - i
    average_time_to_complete = np.mean(times)
    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
    time_remaining = estimated_completion_time - datetime.now()
            
    print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

print(summary_stats)
#portfolios.to_csv('C:/Users/lewis/OneDrive/tradingstrategies/test_csv/portfolios.csv')
#portfolios.to_sql("momentum_portfolios", con = engine, if_exists="replace")

# plt.figure(figsize=(10, 5))
# plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Long Returns'], label='Long Momentum Return')
# plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Short Returns'], label='Short Momentum Return')
# plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Gross Returns'], label='Long/Short Return')
# plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Equal-weighted L/S Returns'], label='Equal-weighted Long/Short Return')
# plt.plot(portfolio_returns.index, portfolio_returns['Cumulative Benchmark Returns'], label='Benchmark Return')

# plt.xlabel('Date')
# plt.ylabel('Return')
# plt.title('Momentum Strategy vs Benchmark Returns')
# plt.legend()
# plt.grid(True)
# plt.xticks(rotation=45)
# plt.show()