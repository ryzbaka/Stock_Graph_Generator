import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import style
import matplotlib.dates as mdates
import datetime as dt
import numpy as np
import pandas_datareader.data as web
from mpl_finance import candlestick_ohlc
from bs4 import BeautifulSoup
import requests
import datetime as dt
import pickle
import os
style.use('ggplot')
def fetchsp500tickers():
    response=requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup=BeautifulSoup(response.text,'lxml')
    table=soup.find('table',{'class':'wikitable sortable'})
    tickers=[]
    for row in table.find_all('tr')[1:]:
        tickers.append(row.find_all('td')[0].text)
    with open('tickers.pickle','wb') as f:
        pickle.dump(tickers,f)
    print('Obtained new tickers for S&P500 companies.')
def get_data(start=dt.datetime(dt.datetime.now().year-5,1,1),stop=dt.datetime(dt.datetime.now().year,dt.datetime.now().month,dt.datetime.now().day),refresh_tickers=False):
    if refresh_tickers:
        fetchsp500tickers()
    successful=[]
    with open('tickers.pickle','rb') as f:
        tickers=pickle.load(f)
    main_df=pd.DataFrame()
    count=0
    try:
        for ticker in tickers[::-1]:
            ticker=ticker[:-1]
            print(ticker)
            df=web.DataReader(ticker,'yahoo',start,stop)
            count+=1
            for col in df.columns:
                df.rename(columns={f'{col}':f'{ticker}_{col}'},inplace=True)
            print(df.head())
            if main_df.empty:
                main_df=df
            else:
                main_df=main_df.join(df,how='outer')
            successful.append(ticker)
            #print(main_df.head())
    except Exception:
        pass
    print(main_df.head())
    for ticker in tickers:
        if ticker not in successful:
            print(f'Error fetching {ticker} due to inavailability of data in provided time frame')
    print('Failed data fetches:',500-count)
    randomval=np.random.rand(1)[0]
    main_df.to_csv(f'collected_data{randomval}.csv')
    name=f'collected_data{randomval}.csv'
    if os.path.exists('fetchlog.pickle'):
        with open('fetchlog.pickle','rb') as f:
            fetchlog=pickle.load(f)
    else:
        fetchlog=[]
    fetchlog.append(name)
    with open('fetchlog.pickle','wb') as f:
        pickle.dump(fetchlog,f)

    with open('tickeroptions.pickle','wb') as f:
        pickle.dump(successful,f)
    failed=[ticker for ticker in tickers if ticker not in successful]
    with open('failedtickers.pickle','wb') as f:
        pickle.dump(failed,f)
    return main_df,successful,failed
def gen_sp_candlestick(ticker,start=dt.datetime(dt.datetime.now().year-5,1,1),stop=dt.datetime(dt.datetime.now().year,dt.datetime.now().month,dt.datetime.now().day),refresh_data=False):
    if refresh_data:
        df,tickeroptions,failedtickers=get_data(start=start,stop=stop,refresh_tickers=True)
    else:
        with open('fetchlog.pickle','rb') as f:
            fetchlog=pickle.load(f)
        latestfile=fetchlog[-1]
        df=pd.read_csv(f'{latestfile}',parse_dates=True,index_col=0)
        with open('tickeroptions.pickle','rb') as f:
            tickeroptions=pickle.load(f)
        with open('failedtickers.pickle','rb') as f:
            failedtickers=pickle.load(f)
    if ticker in tickeroptions:
        columns=[col for col in df.columns if f'{ticker}_' in col]
        df=df[columns]
    else:
        raise Exception('Ticker not found! Try reloading the data or adjusting the time interval')
    resampled=df[f'{ticker}_Adj Close'].resample('5D').ohlc()
    resampled.reset_index(inplace=True)
    resampled['Date']=resampled['Date'].map(mdates.date2num)
    ax1=plt.subplot2grid((6,1),(0,0),rowspan=5,colspan=1)
    ax2=plt.subplot2grid((6,1),(5,0),rowspan=1,colspan=1,sharex=ax1)
    candlestick_ohlc(ax1,resampled.values,width=2,colorup='g')
    ax2.plot(df[f'{ticker}_Volume'])
    plt.show()

gen_sp_candlestick(ticker='ZTS',refresh_data=False)