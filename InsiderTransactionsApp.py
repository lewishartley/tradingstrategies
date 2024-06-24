import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import plotly.graph_objs as go
import streamlit as st

headers = {
    'User-Agent': 'LewisHartley/1.0 (lewisdhartley@icloud.com)'
}

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3" #This is not my API key, I have borrowed it from https://github.com/quantgalore as it seems to be a premium one

cik_mapping_url = 'https://www.sec.gov/files/company_tickers.json'

st.title("Insider Transactions")

st.markdown("This application queries multiple APIs and scrapes and parses web-based forms. Please allow up to a few minutes for it to process, especially for large date ranges. Run speed and other bugs are currently being worked on.")

ticker = st.text_input('Ticker symbol (e.g. AAPL)', 'AAPL')
start_date = st.date_input("Start date", max_value=datetime.today() - timedelta(weeks = 16), value=datetime.today() - timedelta(weeks = 16))
end_date = st.date_input("End date", min_value=start_date + timedelta(days=1), max_value=datetime.today())

filing_or_transaction = st.selectbox(
    'Date Transacted or Date Filed?', ('Date Filed', 'Date Transacted'))

ticker_data = pd.json_normalize(requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()["results"]).set_index("t")
ticker_data.index = pd.to_datetime(ticker_data.index, unit="ms", utc=True).tz_convert("America/New_York")
ticker_data.index = pd.to_datetime(ticker_data.index.date)
ticker_data = ticker_data[["c"]].dropna()
ticker_data.reset_index(inplace=True)
if filing_or_transaction == 'Date Transacted':
    ticker_data = ticker_data.rename(columns={'c' : 'Price', 'index': 'Transaction Date'})
elif filing_or_transaction == 'Date Filed':
    ticker_data = ticker_data.rename(columns={'c' : 'Price', 'index': 'Filing Date'})

# Function to get CIK for a given ticker
def get_cik_for_ticker(ticker):
    try:
        
        response = requests.get(cik_mapping_url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        for entry in data.values():
            if entry['ticker'].upper() == ticker.upper():
                return entry['cik_str']
                
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
    #Function to check whether a cik is valid
def is_valid_cik(cik):
    response = requests.get(f'https://data.sec.gov/submissions/CIK{cik}.json', headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if ticker in data['tickers']:
            return True
        else:
            return False
    else:
        return False
    
    #Function that extracts filings from sec for a given cik
def fetch_filings(cik):
    try:
        # Make the request to get the filings data
        response = requests.get(f'https://data.sec.gov/submissions/CIK{cik}.json', headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None  
    
    #Function that filters extracted filings for form 4s
def filter_form4_filings(filings, start_date, end_date):
    form4_urls = []
    for i in range(len(filings['form'])):
        if filings['form'][i] == "4":
            filing_date = datetime.strptime(filings['filingDate'][i], '%Y-%m-%d')
            filing_date = datetime.date(filing_date)
            if start_date <= filing_date <= end_date:
                form4_urls.append(f"https://www.sec.gov/Archives/edgar/data/{cik}/{filings['accessionNumber'][i].replace('-', '')}/{filings['primaryDocument'][i]}",)
    return form4_urls

# Function to fetch a filing from their url and parse HTML
def fetch_and_parse_html(document_url):
    response = requests.get(document_url, headers=headers)
    response.raise_for_status()
    return BeautifulSoup(response.content, 'html.parser')

# Function to extract relevant data from a filing's html
def extract_form4_data(soup):
    datalist = []
    
    ndtable = soup.find_all('table')
    for i in range(len(ndtable)):
        if ndtable[i].find('tr').get_text(strip=True) == 'Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned':
            non_derivative_table = ndtable[i]
    non_derivative_rows = non_derivative_table.find_all('tr')
    reporting_owner = soup.find_all('table')[5]
    if reporting_owner:
        findtr = reporting_owner.find('tr')
        findtd = findtr.find('td')
        repperson = findtd.find('a').get_text(strip=True)
    
    datetable = soup.find_all('table')[-1]
    if datetable:
        daterow = datetable.find('tr')
        filing_date = daterow.find_all('td')[2].get_text(strip=True)

    if non_derivative_rows:
        for row in non_derivative_rows[3:]:
            data = {}
            findtd = row.find_all('td')
            data['Reporting Person'] = repperson
            data['Filing Date'] = filing_date
            data['Transaction Date'] = findtd[1].get_text(strip=True)
            data['Transaction Code'] = findtd[3].get_text(strip=True)
            data['Transaction Amount'] = findtd[5].get_text(strip=True)
            data['Transaction Price'] = findtd[7].get_text(strip=True)
            datalist.append(data)
    
    return datalist

cik = str(get_cik_for_ticker(ticker))
if is_valid_cik(cik):
    cik=cik
elif is_valid_cik('0' + cik):
    cik = '0'+cik
elif is_valid_cik('00' + cik):
    cik = '00'+cik
elif is_valid_cik('000' + cik):
    cik = '000'+cik
elif is_valid_cik('0000' + cik):
    cik = '0000'+cik
elif is_valid_cik('00000' + cik):
    cik = '00000'+cik
elif is_valid_cik('000000' + cik):
    cik = '000000'+cik
else:
    print("Error fetching CIK")

# Fetch the filings data
filings_data = fetch_filings(cik)
# Check if filings data is fetched successfully
if filings_data:
    # Filter the Form 4 filings within the specified date range
    recent_filings = filings_data.get('filings', {}).get('recent', {})
    urls = filter_form4_filings(recent_filings, start_date, end_date)
    print("No Form 4 filings found within the specified date range.")
else:
    print("Failed to fetch filings data.")

extracted_data = []
for document_url in urls:
    try:
        soup = fetch_and_parse_html(document_url)
        filing_data = extract_form4_data(soup)
        extracted_data = extracted_data + filing_data
    except Exception as e:
        print(f"Error processing {document_url}: {e}")

extracted_data = pd.DataFrame(extracted_data)

extracted_data.replace('', np.nan, inplace=True)
extracted_data.dropna(inplace = True)
extracted_data.reset_index(drop=True, inplace=True)

sells = extracted_data[extracted_data['Transaction Code'].str.contains('S', case=False, na=False)]
sells['Transaction Type'] = "Sell"
buys = extracted_data[extracted_data['Transaction Code'].str.contains('P', case=False, na=False)]
buys['Transaction Type'] = "Buy"
transactions = pd.concat([buys, sells], axis=0).reset_index(drop=True)
transactions = transactions.drop(columns = ['Transaction Code'])

if transactions.empty == False:
    transactions['Transaction Amount'] = transactions['Transaction Amount'].str.replace(',', '', regex=True)
    transactions['Transaction Amount'] = pd.to_numeric(transactions['Transaction Amount'])
    transactions['Transaction Price'] = transactions['Transaction Price'].str.replace(r'\([^)]*\)', '', regex=True)
    transactions['Transaction Price'] = transactions['Transaction Price'][0][1:]   
    transactions['Transaction Price'] = transactions['Transaction Price'].str.replace(',', '', regex=True)
    transactions['Transaction Price'] = pd.to_numeric(transactions['Transaction Price'])

transactions['Transaction Date'] = pd.to_datetime(transactions['Transaction Date'], format='%m/%d/%Y')
transactions['Filing Date'] = pd.to_datetime(transactions['Filing Date'], format='%m/%d/%Y')

transactions.sort_values(by='Transaction Date', ascending = True, inplace=True)
transactions.reset_index(drop=True, inplace=True)

transactions.loc[transactions['Transaction Type'] == 'Buy', 'Transaction Value'] = transactions['Transaction Amount'] * transactions['Transaction Price']
transactions.loc[transactions['Transaction Type'] == 'Sell', 'Transaction Value'] = -transactions['Transaction Amount'] * transactions['Transaction Price']
transactions['Transaction Value'] = transactions['Transaction Value'].astype(int)

date_range = pd.date_range(start=start_date, end=end_date, freq='D')

if filing_or_transaction == 'Date Filed':
    dailydata = pd.DataFrame(date_range, columns=['Filing Date'])

    dailydata = pd.merge(dailydata, transactions[['Filing Date', 'Transaction Value', 'Transaction Type']], 
                    on='Filing Date', how='left')

elif filing_or_transaction == 'Date Transacted':
    dailydata = pd.DataFrame(date_range, columns=['Transaction Date'])

    dailydata = pd.merge(dailydata, transactions[['Transaction Date', 'Transaction Value', 'Transaction Type']], 
                    on='Transaction Date', how='left')

dailydata['Transaction Value'] = dailydata['Transaction Value'].fillna(0).astype(int)
dailydata['Transaction Type'] = dailydata['Transaction Type'].fillna('')

dailydata['Total Buys'] = 0

cumulative_buys = 0
for index, row in dailydata.iterrows():
    if row['Transaction Type'] == 'Buy' and row['Transaction Value'] != 0:
        cumulative_buys += row['Transaction Value']
    dailydata.at[index, 'Total Buys'] = cumulative_buys

dailydata['Total Sells'] = 0

cumulative_sells = 0
for index, row in dailydata.iterrows():
    if row['Transaction Type'] == 'Sell' and row['Transaction Value'] != 0:
        cumulative_sells += row['Transaction Value']
    dailydata.at[index, 'Total Sells'] = cumulative_sells

dailydata['Net Total'] = dailydata['Transaction Value'].cumsum().astype(int)

if filing_or_transaction == 'Date Filed':
    dailydata = pd.merge(dailydata, ticker_data, on = 'Filing Date', how = 'left')
elif filing_or_transaction == 'Date Transacted':
    dailydata = pd.merge(dailydata, ticker_data, on = 'Transaction Date', how = 'left')

dailydata['Price'] = dailydata['Price'].ffill()
dailydata['Price'] = dailydata['Price'].bfill()

show_buys = st.checkbox("Show Cumulative Buys", value=False)
show_sells = st.checkbox("Show Cumulative Sells", value=False)
show_net = st.checkbox("Show Net Total", value=True)
show_underlying = st.checkbox("Show Underlying Stock Price", value=True)

fig = go.Figure()

if filing_or_transaction == 'Date Transacted':
    tracenet = go.Scatter(x=dailydata['Transaction Date'], y=dailydata['Net Total'].values, mode='lines', name='Net Total', line=dict(color='Blue'), showlegend=True, yaxis='y1')
    tracebuys = go.Scatter(x=dailydata['Transaction Date'], y=dailydata['Total Buys'].values, mode='lines', name='Cumulative Buys', line=dict(color='Green'), showlegend=True, yaxis='y1')
    tracesells = go.Scatter(x=dailydata['Transaction Date'], y=dailydata['Total Sells'].values, mode='lines', name='Cumulative Sells', line=dict(color='Red'), showlegend=True, yaxis='y1')
    tracestock = go.Scatter(x=dailydata['Transaction Date'], y=dailydata['Price'].values, mode='lines', name='Stock Price', line=dict(color='Orange'), showlegend=True, yaxis='y2', )
elif filing_or_transaction == 'Date Filed':
    tracenet = go.Scatter(x=dailydata['Filing Date'], y=dailydata['Net Total'].values, mode='lines', name='Net Total', line=dict(color='Blue'), showlegend=True, yaxis='y1')
    tracebuys = go.Scatter(x=dailydata['Filing Date'], y=dailydata['Total Buys'].values, mode='lines', name='Cumulative Buys', line=dict(color='Green'), showlegend=True, yaxis='y1')
    tracesells = go.Scatter(x=dailydata['Filing Date'], y=dailydata['Total Sells'].values, mode='lines', name='Cumulative Sells', line=dict(color='Red'), showlegend=True, yaxis='y1')
    tracestock = go.Scatter(x=dailydata['Filing Date'], y=dailydata['Price'].values, mode='lines', name='Stock Price', line=dict(color='Orange'), showlegend=True, yaxis='y2', )

if show_net:
    fig.add_trace(tracenet)

if show_buys:
    fig.add_trace(tracebuys)

if show_sells:
    fig.add_trace(tracesells)

if show_underlying:
    fig.add_trace(tracestock)


fig.update_layout(
    title= f'{ticker} Insider Transactions',
    xaxis_title='Date',
    yaxis_title='Notional Value Traded',
    template='plotly_white',
    title_x=0.5,
    yaxis=dict(
        title='Value Traded',
        titlefont=dict(color='Black'),
        tickfont=dict(color='Black'),
        showgrid=True,
    ),
    yaxis2=dict(
        title='Stock Price',
        titlefont=dict(color='Black'),
        tickfont=dict(color='Black'),
        overlaying='y',
        side='right',
        showgrid=False,
    ),
    xaxis=dict(
        showgrid=True,
    ),
)

if show_net or show_buys or show_sells or show_underlying:
    st.plotly_chart(fig)

st.dataframe(transactions)
