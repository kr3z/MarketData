import time
import traceback
import pandas as pd
from datetime import date, timedelta
import concurrent.futures
from yahoo_fin.stock_info import get_data
from DB import DBConnection
import logging

logger = logging.getLogger('Yahoo')

last_req: int = 0
REQ_WAIT_TIME: int = 10000
BACKFILL_PERIOD_DAYS: int = 1
PERSIST_THREADS: int = 3
GET_DATES_QUERY = "SELECT s.id,s.symbol,max(yhd.quote_date) FROM Symbol s LEFT OUTER JOIN YahooHistoricalData yhd on s.id = yhd.symbol_key GROUP BY s.id,s.symbol"
INSERT_SQL = "INSERT INTO YahooHistoricalData(id,symbol_key,quote_date,open,high,low,close,adjclose,volume) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
persist_pool =  concurrent.futures.ThreadPoolExecutor(max_workers=PERSIST_THREADS, thread_name_prefix='YahooDataPersister')

def get_historical_data(symbol: str, start_date: date = None):
    global last_req
    start_date_str = start_date.strftime("%m/%d/%Y") if start_date else None
    time_since_last_req = round(time.time() * 1000) - last_req 
    if time_since_last_req < REQ_WAIT_TIME:
        sleep_time = (REQ_WAIT_TIME - time_since_last_req)/1000.0
        logger.debug("Sleeping for API rate limit: %d ms" ,sleep_time*1000)
        time.sleep(sleep_time)
    logger.debug("Getting data for %s starting at %s", symbol,start_date_str)
    try:
        historical_data = get_data(symbol,start_date = start_date_str, index_as_date=False)
    except Exception as error:
        logger.error(error)
        historical_data = None
    last_req = round(time.time() * 1000)

    return historical_data

def backfill_data():
    conn = DBConnection.getConnection()
    res = conn.executeQuery(GET_DATES_QUERY)
    for symbol_key,symbol,last_date in res:
        if last_date is None or (date.today() - last_date).days >= BACKFILL_PERIOD_DAYS:
            if last_date:
                last_date = last_date + timedelta(days=1)
            logger.info("Getting data for symbol: %s", symbol)
            historical_data = get_historical_data(symbol,last_date)
            if historical_data is None:
                continue
            if last_date:
                historical_data = historical_data.loc[historical_data['date']>=pd.Timestamp(last_date)]
            rows_size = historical_data.shape[0]
            if rows_size == 0:
                logger.info("No new data for: %s", symbol)
                continue
            new_ids = DBConnection.get_ids(rows_size)
            historical_data.drop(labels='ticker',axis=1, inplace=True)
            historical_data = historical_data.astype(object).where(pd.notnull(historical_data), None)
            historical_data.insert(0,'id',new_ids, allow_duplicates=False)
            historical_data.insert(1,'symbol_key',symbol_key)
            #logger.debug(historical_data.head)
            persist_pool.submit(persist_yahoo_data,historical_data.to_numpy().tolist())

def persist_yahoo_data(data):
    conn = None
    try:
        logger.debug("Persisting Yahoo batch of %d rows" , len(data))
        conn = DBConnection.getConnection()
        conn.executemany(INSERT_SQL,data)
        conn.commit()
    except Exception as error:
        logger.error(error)
        traceback.print_exc()
        if conn is not None:
            conn.rollback()