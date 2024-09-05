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

risk_free_rate = pd.DataFrame()
times = []

for date in dates:

    start_time = datetime.now()
    
    rf = {'Rate': pd.to_numeric(requests.get(f'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?filter=security_desc:eq:Treasury Bills&filter=record_date:lte:{date}&sort=-record_date&page[size]=1').json()['data'][0]['avg_interest_rate_amt'])}
    rf_df = pd.DataFrame(rf, index=[date])
    risk_free_rate = pd.concat([risk_free_rate, rf_df])

    end_time = datetime.now()
    seconds_to_complete = (end_time - start_time).total_seconds()
    times.append(seconds_to_complete)
    iteration = round((np.where(dates==date)[0][0]/len(dates))*100,2)
    iterations_remaining = len(dates) - np.where(dates==date)[0][0]
    average_time_to_complete = np.mean(times)
    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
    time_remaining = estimated_completion_time - datetime.now()
    
    print(f"Risk-free rate data {iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

risk_free_rate.to_sql("risk_free_rate", con = engine, if_exists = "replace")