#import os
import time
import finnhub
import logging
#import logging.config
import traceback
#import configparser
import pandas as pd
#import concurrent.futures
from requests.exceptions import ReadTimeout
from datetime import date, timedelta, datetime, time as time_
from typing import Tuple, List, Any, Dict, Optional, Callable, Set, cast

from sqlalchemy import select, func, or_
#from sqlalchemy.orm import 

from Base import Session, API_KEY
from FinnHubClasses import FinnHubQuote, MarketStatus, StockSymbol

logger = logging.getLogger('FinnHub')

REQ_WAIT_TIME: int = 1000
#BACKFILL_PERIOD_DAYS: int = 1

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
        except (finnhub.FinnhubAPIException, finnhub.FinnhubRequestException, ReadTimeout) as ex:
            logger.error("FinnHub request failed")
            logger.error(ex)
            if isinstance(ex,finnhub.FinnhubAPIException) and ex.status_code==403:
                logger.error("403 Error encountered for request. Skipping request and continuing")
            else:
                except_sleep_time = 1
                if isinstance(ex,ReadTimeout):
                    except_sleep_time = 10
                logger.error("Opening new FinnHub client")
                self._finnhub_client.close()
                time.sleep(except_sleep_time)
                self._finnhub_client = finnhub.Client(api_key=API_KEY)
                logger.error("retrying request")
                ret = func(self,*args, **kwargs)

        self.__class__._last_req = round(time.time() * 1000)

        return ret

    return rate_limited_func

#FinnhubAPIException
class MarketOpenException(Exception):
    message: str
    cur_time: time_
    def __init__(self, cur_time: time_):
        self.message = "Market is open"
        super().__init__(self.message)
        self.cur_time = cur_time

    def __str__(self):
        return f"{self.message} (Time: {self.cur_time})"

class FinnHubClientWrapper:
    _last_req: int = 0

    _finnhub_client: Optional[finnhub.Client]
    _market_status: Optional[MarketStatus]

    def __init__(self):
        self._finnhub_client = None
        self._market_status = self.market_status()

    @rate_limit
    def update_stock_symbols(self) -> None:

        stock_symbols = self._finnhub_client.stock_symbols('US')
        symbols_df = pd.DataFrame(stock_symbols)
        symbols_df['uid'] = symbols_df['symbol'] + symbols_df['mic']
        symbols_df.set_index('uid', inplace=True)
        uids = symbols_df.index.to_list()
        existing_symbols = cast(Set[StockSymbol],StockSymbol.get_all_from_cache(uids, 'uid'))
        existing_uids = [existing_symbol.uid for existing_symbol in existing_symbols]
        new_uids = [uid for uid in uids if uid not in existing_uids]

        logger.info(f"Found {len(new_uids)} New Symbols")
        logger.info(f"Comparing {len(existing_uids)} Existing Symbol")

        with Session() as session:
            for symbol in existing_symbols:
                if symbol.compare(symbols_df.loc[symbol.uid]):
                    session.merge(symbol)
            session.flush()

            new_symbols = StockSymbol.parse_data(symbols_df.loc[new_uids])
            session.add_all(new_symbols)
            session.flush()

            no_longer_finnhub_symbols = session.scalars(select(StockSymbol).where(StockSymbol.finnhub_symbol==1, StockSymbol.uid.not_in(uids))).all()
            logger.info(f"Found {len(no_longer_finnhub_symbols)} Symbols that are no longer finnhub symbols")
            for symbol in no_longer_finnhub_symbols:
                symbol.finnhub_symbol = 0
            
            session.commit()

    @rate_limit
    def market_status(self) -> MarketStatus:
        status_data = self._finnhub_client.market_status(exchange='US')
        status = MarketStatus(status_data)
        return status
    
    @rate_limit
    def get_quote(self, symbol: str) -> FinnHubQuote:
        quote_data = self._finnhub_client.quote(symbol)
        quote = None
        if int(quote_data.get("t")) > 0:
            quote = FinnHubQuote(quote_data)
        return quote

    def update_quotes(self) -> None:
        quote_day: date = date.today()
        update_time = datetime.combine(quote_day, time_(hour=19, minute=0, second=0))
        current_time = datetime.now().time()

        if current_time < update_time.time():
            quote_day = quote_day - timedelta(days=1)
        #elif self.market_status().holiday is not None:
        elif self._market_status.holiday is not None:
            quote_day = quote_day - timedelta(days=1)
            logger.info("Today is a Holiday, updating quotes for %s instead",quote_day)
        
        weekday_diff = quote_day.weekday() - 4
        if weekday_diff > 0:
            logger.info("%s is a weekend",quote_day)
            quote_day = quote_day - timedelta(days=weekday_diff)
            logger.info("Updating quotes for %s instead",quote_day)
        
        stmt = select(StockSymbol.id)\
                .where(StockSymbol.finnhub_symbol ==1 )\
                .where(or_(StockSymbol.last_finnhub_quote_check < update_time,
                           StockSymbol.last_finnhub_quote_check == None))
        
        with Session() as session:
            res = session.scalars(stmt).all()
            ids = [id for id in res]

        try:
            for id in ids:
                if not self._market_status.is_valid:
                    self._market_status = self.market_status()
                if self._market_status.is_open:
                    raise MarketOpenException(self._market_status.last_checked)
                symbol = cast(StockSymbol,StockSymbol.get_from_cache(id,'id'))
                logger.debug(f"Getting quote for symbol: {symbol.symbol}")
                quote = cast(FinnHubQuote,self.get_quote(symbol.symbol))
                with Session() as session:
                    if quote and (symbol.last_finnhub_quote_check is None or quote.quote_time > symbol.last_finnhub_quote_check):
                    #if quote and (symbol.last_finnhub_quote_check is None or quote.quote_time > datetime.combine(symbol.last_finnhub_quote_check, datetime.min.time())):
                        logger.debug(f"Persisting quote for symbol: {symbol.symbol}")
                        quote.symbol_key = symbol.id
                        quote.symbol = symbol
                        # Normally it would be better to collect these quotes and insert them as a batch to prevent excessive inserts
                        # But we have to wait 1 sec per API call so it doesn't really matter
                        session.add(quote)
                        session.flush()
                        logger.debug(f"Persisted quote for symbol: {symbol.symbol}")

                    logger.debug(f"Updating last check time for symbol: {symbol.symbol}")
                    symbol = session.merge(symbol)
                    StockSymbol.add_to_cache(symbol)
                    symbol.last_finnhub_quote_check = datetime.now()
                    #symbol.last_finnhub_quote_check = quote_day
                    session.commit()
        except MarketOpenException:
            logger.error("Market Open, exiting")