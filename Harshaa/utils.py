import datetime
import pandas as pd
from pymongo import MongoClient
conn_url = 'mongodb://{}:{}@{}:{}/{}?authMechanism=SCRAM-SHA-1'

def connect_mongo(host,port,db,user,pswd):
    return MongoClient(conn_url.format(user, pswd, host, port, db))[db]
	
def init():	
	identity = connect_mongo('tgl-mongodb21.rctanalytics.com','27017','identity','st_market_int','9am4Lewk9s0a')
	siteref = connect_mongo('tgl-mongodb21.rctanalytics.com','27017','siteref_copyofprod','st_market_int','k59cPkRd8xk2')
	market_intelligence = connect_mongo('tgl-mongodb11.rctanalytics.com','27017','market_intelligence','st_market_int','DbMGP0GBiBw2')
	exl_tag = connect_mongo('tgl-mongodb11.rctanalytics.com','27017','exl_tag','st_exl_tag','m8bPV5NuQN04')
		
	return identity,siteref,market_intelligence,exl_tag
