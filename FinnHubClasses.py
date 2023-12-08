import time
import hashlib
import traceback
import pandas as pd
from datetime import date, timedelta, datetime
from typing import Tuple, List, Any, Dict, Optional

from DB import DBConnection
import logging

logger = logging.getLogger('FinnHub')

def cacheinit(cls):
    cls._id_map = {}
    cls._hashes = set()
    cls._date_map = {}

    conn = DBConnection.getConnection()
    result = conn.executeQuery(cls._cache_sql)
    for c in result:
        cls._id_map[c[1]]=c[0]
        cls._hashes.add(c[2])
        cls._date_map[c[1]] = c[3]

    cls.cache_init()

    return cls

class MarketStatus:
    def __init__(self,status):
        self.holiday = status.get("holiday")
        self.isOpen = status.get("isOpen")
        self.session = status.get("session")
        self.timezone = status.get("timezone")
        self.t = datetime.fromtimestamp(int(status.get("t")))

@cacheinit
class StockSymbol:
    _cache_sql = "SELECT id,uid,hash,update_time FROM Symbol"
    _insert_sql = "INSERT INTO Symbol(currency,description,displaySymbol,figi,mic,shareClassFIGI,symbol,symbol2,type,hash,update_time,id) VALUES(" + ','.join(["%s"]*12) + ')'
    _update_sql = "UPDATE Symbol SET currency=%s,description=%s,displaySymbol=%s,figi=%s,mic=%s,shareClassFIGI=%s,symbol=%s,symbol2=%s,type=%s,hash=%s,update_time=%s,update_count=update_count+1 WHERE id=%s"

    @classmethod
    def cache_init(cls):
        pass

    def __init__(self, symbol_data):
        self.currency: str = symbol_data.get("currency")
        self.description: str = symbol_data.get("description")
        self.displaySymbol: str = symbol_data.get("displaySymbol")
        self.figi: str = symbol_data.get("figi")
        self.mic: str = symbol_data.get("mic")
        self.shareClassFIGI: str = symbol_data.get("shareClassFIGI")
        self.symbol: str = symbol_data.get("symbol")
        self.symbol2: str = symbol_data.get("symbol2")
        self.type: str = symbol_data.get("type")
        self.data_date = datetime.now()

        # Symbol itself may no be unique across exchanges
        # Combination of symbol and mic should be a unique identifier
        self._uid = ''.join((self.symbol,self.mic))

        self._md5 = hashlib.md5(''.join(str(field) for field in self.get_hash_data()).encode('utf-8')).hexdigest()

        self._exists : bool = self._uid in self.__class__._id_map
        self._needs_update : bool = False

        if not self.exists():
            logger.debug("Symbol %s for exchange %s not found in DB. Persisting" ,self.symbol, self.mic)
            self.__class__._id_map[self._uid] = DBConnection.get_next_id()
        elif self._md5 not in self.__class__._hashes:
            logger.debug("Symbol %s for exchange %s needs updating" ,self.symbol, self.mic)
            self._needs_update = True
            

        self._id = self.__class__._id_map.get(self._uid)
        self.__class__._hashes.add(self._md5)
        #self.__class__._date_map[self._uid] = self.data_date


    def exists(self) -> bool:
        return self._exists
    
    def needs_update(self) -> bool:
        return self._needs_update

    def get_hash_data(self) -> Tuple[Any]:
        return (self.currency,self.description,self.displaySymbol,self.figi,self.mic,self.shareClassFIGI,self.symbol,self.symbol2,self.type)
    
    def get_persist_data(self) -> Tuple[Any]:
        return self.get_hash_data() + (self._md5, self.data_date, self._id)
    
@cacheinit
class Exchange:
    _cache_sql = "SELECT id,mic,hash,update_time FROM Exchange"
    _insert_sql = "INSERT INTO Exchange(mic,operating_mic,oprt_sgmt,market_name,legal_entity_name,lei,market_category_code,acronym,iso_country_code,city,website,status,creation_date,last_update_date,last_validation_date,expiry_date,comments,hash,update_time,id) VALUES(" + ','.join(["%s"]*20) + ')'
    _update_sql = "UPDATE Exchange SET mic=%s,operating_mic=%s,oprt_sgmt=%s,market_name=%s,legal_entity_name=%s,lei=%s,market_category_code=%s,acronym=%s,iso_country_code=%s,city=%s,website=%s,status=%s,creation_date=%s,last_update_date=%s,last_validation_date=%s,expiry_date=%s,comments=%s,hash=%s,update_time=%s,update_count=update_count+1 WHERE id=%s"


    @classmethod
    def cache_init(cls):
        pass

    @classmethod
    def parse_exchanges(cls, exchange_data: pd.DataFrame) -> List['Exchange']:
        exchanges = []
        for _, row in exchange_data.iterrows():
            exchanges.append(Exchange(row))
        return exchanges

    def __init__(self, exchange_data: pd.Series):
        self.mic: str = exchange_data['MIC']
        self.operating_mic: str = exchange_data['OPERATING MIC']
        self.oprt_sgmt: str = exchange_data['OPRT/SGMT']
        self.market_name: str = exchange_data['MARKET NAME-INSTITUTION DESCRIPTION']
        self.legal_entity_name: str = exchange_data['LEGAL ENTITY NAME']
        self.lei: str = exchange_data['LEI']
        self.market_category_code: str = exchange_data['MARKET CATEGORY CODE']
        self.acronym: str = exchange_data['ACRONYM']
        self.iso_country_code: str = exchange_data['ISO COUNTRY CODE (ISO 3166)']
        self.city: str = exchange_data['CITY']
        self.website: str = exchange_data['WEBSITE']
        self.status: str = exchange_data['STATUS']
        self.creation_date: date = date.fromisoformat(str(exchange_data['CREATION DATE'])) if exchange_data['CREATION DATE'] else None
        self.last_update_date: date = date.fromisoformat(str(exchange_data['LAST UPDATE DATE'])) if exchange_data['LAST UPDATE DATE'] else None
        self.last_validation_date: date = date.fromisoformat(str(exchange_data['LAST VALIDATION DATE'])) if exchange_data['LAST VALIDATION DATE'] else None
        self.expiry_date: date = date.fromisoformat(str(exchange_data['EXPIRY DATE'])) if exchange_data['EXPIRY DATE'] else None
        self.comments: str = exchange_data['COMMENTS']
        self.data_date: date = datetime.now()

        self._md5 = hashlib.md5(''.join(str(field) for field in self.get_hash_data()).encode('utf-8')).hexdigest()

        self._exists : bool = self.mic in self.__class__._id_map
        self._needs_update : bool = False

        if not self.exists():
            logger.debug("Exchange %s not found in DB. Persisting" , self.mic)
            self.__class__._id_map[self.mic] = DBConnection.get_next_id()
        elif self._md5 not in self.__class__._hashes:
            logger.debug("Exchange %s needs updating" , self.mic)
            self._needs_update = True
            

        self._id = self.__class__._id_map.get(self.mic)
        self.__class__._hashes.add(self._md5)

    def exists(self) -> bool:
        return self._exists
    
    def needs_update(self) -> bool:
        return self._needs_update

    def get_hash_data(self) -> Tuple[Any]:
        return (self.mic,self.operating_mic,self.oprt_sgmt,self.market_name,self.legal_entity_name,self.lei,self.market_category_code,self.acronym,self.iso_country_code,self.city,self.website,self.status,self.creation_date,self.last_update_date,self.last_validation_date,self.expiry_date,self.comments)
    
    def get_persist_data(self) -> Tuple[Any]:
        return self.get_hash_data() + (self._md5, self.data_date, self._id)
    
class FinnHubQuote:
    _insert_sql = "INSERT INTO FinnHubQuote(symbol_key,current_price,day_change,percent_change,high,low,open,previous_close,quote_time,id) VALUES(" + ','.join(["%s"]*10) + ')'
    def __init__(self,quote_data, symbol_key):
        self.current_price: float = quote_data.get('c')
        self.day_change: float = quote_data.get('d')
        self.percent_change: float = quote_data.get('dp')
        self.high: float = quote_data.get('h')
        self.low: float = quote_data.get('l')
        self.open: float = quote_data.get('o')
        self.previous_close: float = quote_data.get('pc')
        self.quote_time: datetime = datetime.fromtimestamp(int(quote_data.get("t")))

        self.symbol_key = symbol_key
        self._id = DBConnection.get_next_id()

    def get_persist_data(self) -> Tuple[Any]:
        return (self.symbol_key,self.current_price, self.day_change, self.percent_change, self.high, self.low, self.open, self.previous_close, self.quote_time, self._id)

""" @cacheinit
class CompanyProfile:
    #_cache_sql = "SELECT id,uid,hash,update_time FROM Symbol"
    #_insert_sql = "INSERT INTO Symbol(currency,description,displaySymbol,figi,mic,shareClassFIGI,symbol,symbol2,type,hash,update_time,id) VALUES(" + ','.join(["%s"]*12) + ')'
    #_update_sql = "UPDATE Symbol SET currency=%s,description=%s,displaySymbol=%s,figi=%s,mic=%s,shareClassFIGI=%s,symbol=%s,symbol2=%s,type=%s,hash=%s,update_time=%s,update_count=update_count+1 WHERE id=%s"

    @classmethod
    def cache_init(cls):
        pass

    def __init__(self, profile_data):
        self.data_date = datetime.now()
        self.country: str = profile_data.get("country")
        self.currency: str = profile_data.get("currency")
        self.estimateCurrency: str = profile_data.get("estimateCurrency")
        self.exchange: str = profile_data.get("exchange")
        self.ipo: date = date.fromisoformat(profile_data.get("ipo"))
        self.marketCapitalization: float = profile_data.get("marketCapitalization")
        self.name: str = profile_data.get("name")
        self.phone: str = profile_data.get("phone")
        self.shareOutstanding: float = profile_data.get("shareOutstanding")
        self.ticker: str = profile_data.get("ticker")
        self.weburl: str = profile_data.get("weburl")
        self.logo: str = profile_data.get("logo")
        self.finnhubIndustry: str = profile_data.get("finnhubIndustry")

        self._uid
        self.exhange_key
        self.symbol_key

        self._md5 = hashlib.md5(''.join(str(field) for field in self.get_hash_data()).encode('utf-8')).hexdigest()

        self._exists : bool = self._uid in self.__class__._id_map
        self._needs_update : bool = False

        if not self.exists():
            logger.debug("Profile for symbol %s and exchange %s not found in DB. Persisting" ,self.ticker, self.exchange)
            self.__class__._id_map[self._uid] = DBConnection.get_next_id()
        elif self._md5 not in self.__class__._hashes:
            logger.debug("Profile for symbol %s and exchange %s needs updating" ,self.ticker, self.exchange)
            self._needs_update = True
            

        self._id = self.__class__._id_map.get(self._uid)
        self.__class__._hashes.add(self._md5)
        #self.__class__._date_map[self._uid] = self.data_date


    def exists(self) -> bool:
        return self._exists
    
    def needs_update(self) -> bool:
        return self._needs_update

    def get_hash_data(self) -> Tuple[Any]:
        return (self.currency,self.description,self.displaySymbol,self.figi,self.mic,self.shareClassFIGI,self.symbol,self.symbol2,self.type)
    
    def get_persist_data(self) -> Tuple[Any]:
        return self.get_hash_data() + (self.exhange_key, self.symbol_key, self._md5, self.data_date, self._id) """