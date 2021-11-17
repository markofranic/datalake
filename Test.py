import numpy as np
import datetime
import pandas as pd
from psaw import PushshiftAPI
import psycopg2

conn = psycopg2.connect("host=datalake.cxcqywp4y4sf.us-east-1.rds.amazonaws.com dbname=datalake1 user=postgres password=postgres123")
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute("CREATE TABLE test123 (author VARCHAR,created_utc VARCHAR, domain VARCHAR,over_18 VARCHAR,selftext VARCHAR,title VARCHAR,subreddit VARCHAR);")
cur.close()
conn.close()