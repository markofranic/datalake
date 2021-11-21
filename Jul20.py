import numpy as np
import datetime
import pandas as pd
from psaw import PushshiftAPI
import psycopg2
conn = psycopg2.connect("host=datalake.cxcqywp4y4sf.us-east-1.rds.amazonaws.com dbname=datalake1 user=postgres password=postgres123")
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute("CREATE TABLE redditposts (author VARCHAR,created_utc VARCHAR, domain VARCHAR,over_18 VARCHAR,selftext VARCHAR,title VARCHAR,subreddit VARCHAR);")
cur.execute("SELECT DISTINCT name FROM crypto")
cryptocurrencies = (cur.fetchall())
cur.close()
conn.close()
tempcryptolist = list()
for i in cryptocurrencies:
    tempcryptolist.append(str(i))
cryptolist=list()
removers = ["(", ")",",","'"]
for i in tempcryptolist:
    for character in removers:
        i=i.replace(character,'')
    cryptolist.append(i)
api = PushshiftAPI()
subreddits = ['CryptoCurrency','CryptoMarkets','CryptoCurrencyTrading','CryptoCurrencies']
for i in subreddits:
    cryptolist.append(i)
start_epoch=int(datetime.datetime(2020, 7, 1).timestamp())
end_epoch=int(datetime.datetime(2020, 8, 1).timestamp())
for i in cryptolist:
    conn = psycopg2.connect("host=datalake.cxcqywp4y4sf.us-east-1.rds.amazonaws.com dbname=datalake1 user=postgres password=postgres123")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    listofposts = list()
    query = list(api.search_submissions(after=start_epoch, before=end_epoch,
                            subreddit= str(i), filter=['author', 'domain','over_18','selftext','title','subreddit']))
    for element in query:
        listofposts.append(element.d_)
    dftest=pd.DataFrame(listofposts)
    for i in list(dftest):
        if i == 'over_18' or i == 'created_utc' or i == 'author':
            dftest[str(i)] = dftest[str(i)] .apply(lambda x :str(x))
        elif i == 'selftext' or i == 'title':
            dftest[str(i)] = dftest[str(i)] .apply(lambda x :x.replace("'", "") if type(x) == str else x)
        else:
            pass
    dftest = dftest.replace(np.nan, '', regex=True)
    for i in list(dftest):
        if i == 'created':
            dftest = dftest.drop('created',axis=1)
    for i in range(dftest.shape[0]):
        sqlstring = "INSERT INTO redditposts VALUES ('" +"', '".join( dftest.iloc[i,:])+ "')"
        cur.execute(sqlstring)
    cur.close()
    conn.close()
