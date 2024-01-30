import os
import time
import finnhub
import logging
import logging.config
import traceback
import configparser
import pandas as pd
import concurrent.futures
from datetime import date, timedelta, datetime
from typing import Tuple, List, Any, Dict, Optional, Callable, Set

from DB import DBConnection
from FinnHubClasses import FinnHubQuote, MarketStatus, StockSymbol

WORKING_DIR = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
config.read(WORKING_DIR+os.sep+'config.properties')
API_KEY = config.get("api", "api_key")

logging.config.fileConfig(WORKING_DIR+os.sep+'logging.conf')
logger = logging.getLogger('FinnHub')

REQ_WAIT_TIME: int = 1000
BACKFILL_PERIOD_DAYS: int = 1
PERSIST_THREADS: int = 3
BATCH_SIZE: int = 100

persist_pool =  concurrent.futures.ThreadPoolExecutor(max_workers=PERSIST_THREADS, thread_name_prefix='DataPersister')

class DataQueue:
    def __init__(self, pool: concurrent.futures.Executor, type_, batch_size : int = BATCH_SIZE):
        self._insert_data: List[Tuple[Any]] = []
        self._update_data: List[Tuple[Any]] = []
        self._pool: concurrent.futures.Executor = pool
        self._type = type_
        self._batch_size: int = batch_size

        self._insert_count: int = 0
        self._update_count: int = 0

    def _submit(self,func: Callable[[str,List[Tuple[Any]]],None], sql: str, data: List[Tuple[Any]]):
        self._pool.submit(func, sql, data[:])
        del data[:]

    def insert(self,data: Tuple[Any]) -> None:
        self._insert_data.append(data)
        if len(self._insert_data) == self._batch_size:
            self._insert_count += len(self._insert_data)
            logger.debug("Submitting %s insert batch with size: %d", self._type.__name__, len(self._insert_data))
            self._submit(persist_data,self._type._insert_sql,self._insert_data)

    def update(self,data: Tuple[Any]) -> None:
        self._update_data.append(data)
        if len(self._update_data) == self._batch_size:
            self._update_count += len(self._update_data)
            logger.debug("Submitting%s update batch with size: %d", self._type.__name__, len(self._update_data))
            self._submit(persist_data,self._type._update_sql,self._update_data)

    def close(self) -> None:
        if self._insert_data:
            self._insert_count += len(self._insert_data)
            logger.debug("Submitting %s insert batch with size: %d", self._type.__name__, len(self._insert_data))
            self._submit(persist_data,self._type._insert_sql,self._insert_data)

        if self._update_data:
            self._update_count += len(self._update_data)
            logger.debug("Submitting %s update batch with size: %d", self._type.__name__, len(self._update_data))
            self._submit(persist_data,self._type._update_sql,self._update_data)

def persist_data(sql: str, data: List[Tuple[Any]]) -> None:
    conn = None
    try:
        conn = DBConnection.getConnection()
        conn.executemany(sql,data)
        conn.commit()
    except Exception as error:
        logger.error(error)
        traceback.print_exc()
        if conn is not None:
            conn.rollback()

def rate_limit(func: Callable):
    def rate_limited_func(self,*args, **kwargs):
        if self._finnhub_client is None:
            logger.debug("Opening new finnhub Client")
            self._finnhub_client = finnhub.Client(api_key=API_KEY)
    
        time_since_last_req = round(time.time() * 1000) - self.__class__._last_req
        if time_since_last_req < REQ_WAIT_TIME:
            sleep_time = (REQ_WAIT_TIME - time_since_last_req)/1000.0
            logger.debug("Sleeping for API rate limit: %d ms" ,sleep_time*1000)
            time.sleep(sleep_time)

        ret = None
        try:
            ret = func(self,*args, **kwargs)
        except (finnhub.FinnhubAPIException, finnhub.FinnhubRequestException) as ex:
            logger.error("FinnHub request failed")
            logger.error(ex)
            if isinstance(ex,finnhub.FinnhubAPIException) and ex.status_code==403:
                logger.error("403 Error encountered for request. Skipping request and continuing")
            else:
                logger.error("Opening new FinnHub client")
                self._finnhub_client.close()
                time.sleep(1)
                self._finnhub_client = finnhub.Client(api_key=API_KEY)
                logger.error("retrying request")
                ret = func(self,*args, **kwargs)

        self.__class__._last_req = round(time.time() * 1000)

        return ret

    return rate_limited_func

class FinnHubClientWrapper:
    _last_req: int = 0
    def __init__(self):
        self._finnhub_client: finnhub.Client = None

#FinnhubAPIException

    @rate_limit
    def update_stock_symbols(self) -> None:
        sym_queue = DataQueue(persist_pool,StockSymbol,BATCH_SIZE)
        finnhub_symbol_ids: Set[int] = set()
        conn = DBConnection.getConnection()
        res = conn.executeQuery("SELECT id FROM Symbol WHERE FinnHubSymbol=1")
        for row in res:
            finnhub_symbol_ids.add(row[0])

        stock_symbols = self._finnhub_client.stock_symbols('US')

        #finnhub_symbol_ids: List[Tuple[int]] = []
        for symbol_data in stock_symbols:
            sym = StockSymbol(symbol_data)
            #finnhub_symbol_ids.append((sym._id,))
            if sym._id in finnhub_symbol_ids:
                finnhub_symbol_ids.remove(sym._id)
            if not sym.exists():
                sym_queue.insert(sym.get_persist_data())
            elif sym.needs_update():
                sym_queue.update(sym.get_persist_data())
        sym_queue.close() 

        update_ids: List[int] = list(finnhub_symbol_ids)
        while update_ids:
            id_batch = update_ids[:100]
            update_query = "UPDATE Symbol SET FinnHubSymbol=0 WHERE FinnHubSymbol=1 AND id IN (" + ''.join("%s,")*len(id_batch)
            update_query = update_query[:-1] + ")"
            conn.execute(update_query,id_batch)
            del update_ids[:100]
        conn.commit()
        """ logger.debug("Creating temp id table")
        conn.execute("CREATE TEMPORARY TABLE FinnHubSymbolIds(id int NOT NULL)")
        logger.debug("Inserting symbol ids")
        conn.executemany("INSERT INTO FinnHubSymbolIds(id) VALUES(%s)",finnhub_symbol_ids)
        logger.debug("updating symbols that are no longer in finnhub")
        conn.execute("UPDATE Symbol SET FinnHubSymbol=0 WHERE FinnHubSymbol=1 AND id NOT IN (SELECT id FROM FinnHubSymbolIds)")
        logger.debug("committing transaction")
        conn.commit()
        logger.debug("Dropping temp table")
        conn.execute("DROP TEMPORARY TABLE FinnHubSymbolIds")
        logger.debug("Finished updating finnhub symbols") """

    @rate_limit
    def market_status(self) -> MarketStatus:
        status_data = self._finnhub_client.market_status(exchange='US')
        status = MarketStatus(status_data)
        return status
    
    @rate_limit
    def get_quote(self, symbol: str, symbol_key: int) -> FinnHubQuote:
        quote_data = self._finnhub_client.quote(symbol)
        quote = None
        if int(quote_data.get("t")) > 0:
            quote = FinnHubQuote(quote_data,symbol_key)
        return quote

    def update_quotes(self) -> None:
        quote_day: date = date.today()
        if self.market_status().holiday is not None:
            quote_day = quote_day - timedelta(days=1)
            logger.info("Today is a Holiday, updating quotes for %s instead",quote_day)
        
        weekday_diff = quote_day.weekday() - 4
        if weekday_diff > 0:
            logger.info("%s is a weekend",quote_day)
            quote_day = quote_day - timedelta(days=weekday_diff)
            logger.info("Updating quotes for %s instead",quote_day)
        
    
        quote_queue = DataQueue(persist_pool,FinnHubQuote,BATCH_SIZE)
        conn = DBConnection.getConnection()
        res = conn.executeQuery("SELECT s.id,s.symbol,max(q.quote_time) as quote_time FROM Symbol s LEFT OUTER JOIN FinnHubQuote q on s.id=q.symbol_key WHERE FinnHubSymbol=1 GROUP BY s.id,s.symbol HAVING IFNULL(quote_time,%s) < %s",[date(1900,1,1), quote_day])
        for symbol_key,symbol,quote_time in res:
            if quote_time is None or quote_time.date() < quote_day:
                logger.debug("Getting quote for symbol: %s",symbol)
                quote = self.get_quote(symbol,symbol_key)
                if quote is not None and (quote_time is None or quote.quote_time > quote_time):
                    quote_queue.insert(quote.get_persist_data())
        quote_queue.close()

#@property
""" def finnhub_client() -> finnhub.Client:
    global last_req,_finnhub_client
    if _finnhub_client is None:
        logger.debug("Opening new finnhub Client")
        _finnhub_client = finnhub.Client(api_key=API_KEY)
    time_since_last_req = round(time.time() * 1000) - last_req
    #logger.debug("Time since last req: %s",time_since_last_req)
    if time_since_last_req < REQ_WAIT_TIME:
        sleep_time = (REQ_WAIT_TIME - time_since_last_req)/1000.0
        logger.debug("Sleeping for API rate limit: %d ms" ,sleep_time*1000)
        time.sleep(sleep_time)
    last_req = round(time.time() * 1000)
    return _finnhub_client """

#@property
def market_status() -> MarketStatus:
    raise
    status_data = finnhub_client().market_status(exchange='US')
    status = MarketStatus(status_data)
    return status

""" def update_stock_symbols() -> None:
    sym_queue = DataQueue(persist_pool,StockSymbol,BATCH_SIZE)
    stock_symbols = finnhub_client().stock_symbols('US')
    for symbol_data in stock_symbols:
        sym = StockSymbol(symbol_data)
        if not sym.exists():
            sym_queue.insert(sym.get_persist_data())
        elif sym.needs_update():
            sym_queue.update(sym.get_persist_data())
    sym_queue.close() """

def get_quote(symbol: str, symbol_key: int) -> FinnHubQuote:
    raise
    quote_data = finnhub_client().quote(symbol)
    quote = FinnHubQuote(quote_data,symbol_key)
    return quote

def update_quotes() -> None:
    raise
    today: date = date.today()

    if today.weekday() >= 5 or market_status().holiday is not None:
        logger.info("Market closed for weekend/holiday")
        return
    
    quote_queue = DataQueue(persist_pool,FinnHubQuote,BATCH_SIZE)
    conn = DBConnection.getConnection()
    res = conn.executeQuery("SELECT s.id,s.symbol,max(q.quote_time) FROM Symbol s LEFT OUTER JOIN FinnHubQuote q on s.id=q.symbol_key GROUP BY s.id,s.symbol")
    for symbol_key,symbol,quote_time in res:
        if quote_time is None or quote_time.date() < today:
            logger.debug("Getting quote for symbol: %s",symbol)
            quote = get_quote(symbol,symbol_key)
            if quote.quote_time > quote_time:
                quote_queue.insert(quote.get_persist_data())

#update_quotes()

finnhub_client: FinnHubClientWrapper = FinnHubClientWrapper()
finnhub_client.update_stock_symbols()
finnhub_client.update_quotes()