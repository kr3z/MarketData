import time
import hashlib
import traceback
import pandas as pd
from datetime import date, timedelta, datetime
from typing import Tuple, List, Set, Any, Dict, Optional

from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, Float, UniqueConstraint, Index, Boolean, func, Computed, DateTime, Date, select, text


#from DB import DBConnection
import logging

from Base import Base, Session, engine, get_next_id

logger = logging.getLogger('FinnHub')

# TODO: remove hashing and replace with __eq__ overload

def cacheinit(cls):
    cls._id_map = {}
    cls._hashes = set()
    cls._date_map = {}

    #conn = DBConnection.getConnection()
    with Session() as session:
        existing_data = session.execute(cls.cache_stmt).all()
                #existing_filenames = {existing_file[0].name for existing_file in existing_files}
    #result = conn.executeQuery(cls._cache_sql)
    #for c in result:
    #    cls._id_map[c[1]]=c[0]
    #    cls._hashes.add(c[2])
    #    cls._date_map[c[1]] = c[3]
        
        for row in existing_data:
            #cls._id_map[row[0].uid] = row[0].id
            #cls._hashes.add(row[0].hash)
            #cls._date_map[row[0].uid] = row[0].update_time
            cls._id_map[row[1]] = row[0]
            cls._hashes.add(row[2])
            cls._date_map[row[1]] = row[3]

    #cls.cache_init()

    return cls

class MarketStatus:
    def __init__(self,status):
        self.holiday = status.get("holiday")
        self.isOpen = status.get("isOpen")
        self.session = status.get("session")
        self.timezone = status.get("timezone")
        self.t = datetime.fromtimestamp(int(status.get("t")))

@cacheinit
class StockSymbol(Base):
    __tablename__ = "Symbol"
    #id: Mapped[int] = mapped_column(id_seq,primary_key=True)
    id: Mapped[int] = mapped_column(Integer,primary_key=True)
    currency: Mapped[Optional[str]] = mapped_column(String(30))
    description: Mapped[Optional[str]] = mapped_column(String(500))
    displaySymbol: Mapped[str] = mapped_column(String(30))
    figi: Mapped[Optional[str]] = mapped_column(String(30))
    mic: Mapped[Optional[str]] = mapped_column(String(30))
    shareClassFIGI: Mapped[Optional[str]] = mapped_column(String(30))
    symbol: Mapped[str] = mapped_column(String(30))
    symbol2: Mapped[Optional[str]] = mapped_column(String(30))
    type: Mapped[Optional[str]] = mapped_column(String(500))
    FinnHubSymbol: Mapped[bool] = mapped_column(Boolean, server_default=text("0"))
    hash: Mapped[str] = mapped_column(String(50))
    update_time: Mapped[datetime] = mapped_column(DateTime,server_default=func.CURRENT_TIMESTAMP())
    update_count: Mapped[int] = mapped_column(Integer,server_default=text("0"))

    #TODO: Is this needed? Remove in favor of just using concat
    uid: Mapped[str] = mapped_column(String(30),Computed('concat(symbol,mic)',persisted=False))

    quotes: Mapped[List["FinnHubQuote"]] = relationship(back_populates="symbol", 
                                                        #foreign_keys="FinnHubQuote.symbol_key", 
                                                        primaryjoin="StockSymbol.id == foreign(FinnHubQuote.symbol_key)",
                                                        lazy='selectin', 
                                                        order_by="desc(FinnHubQuote.quote_time)")
                                     
    __table_args__ = (
        UniqueConstraint("uid", name="ux_Symbol_uid"),
        #TODO: replace remove uid and replace with composite ux
        #UniqueConstraint("symbol","mic", name="ux_Symbol_symbol_mic")
    )

    cache_stmt = select(id,uid,hash,update_time)
    # TODO: remove uid and use concat
    #cache_stmt = select(id,func.concat(symbol,mic),hash,update_time)
    #_cache_sql = "SELECT id,uid,hash,update_time FROM Symbol"
    
    #_insert_sql = "INSERT INTO Symbol(currency,description,displaySymbol,figi,mic,shareClassFIGI,symbol,symbol2,type,FinnHubSymbol,hash,update_time,id) VALUES(" + ','.join(["%s"]*13) + ')'
    #_update_sql = "UPDATE Symbol SET currency=%s,description=%s,displaySymbol=%s,figi=%s,mic=%s,shareClassFIGI=%s,symbol=%s,symbol2=%s,type=%s,FinnHubSymbol=%s,hash=%s,update_time=%s,update_count=update_count+1 WHERE id=%s"

    #@classmethod
    #def cache_init(cls):
    #    pass

    @classmethod
    def parse_symbols(cls, symbols_data: List[Dict]) -> Tuple[Dict[int,Dict],List["StockSymbol"]]:
        #existing_ids: Set[int] = set()
        existing_data: Dict[int,Dict] = {}
        new_symbols: List["StockSymbol"] = []
        for symbol_data in symbols_data:
            symbol = symbol_data.get("symbol")
            mic = symbol_data.get("mic")
            uid = ''.join((symbol,mic))
            id = cls._id_map.get(uid)
            if id:
                #existing_ids.add(id)
                existing_data[id] = symbol_data
            else:
                sym = cls(symbol_data)
                new_symbols.append(sym)
                
        return (existing_data,new_symbols)
    
    def compare(self, symbol_data):
        changed: bool = False

        if self.currency != symbol_data.get("currency"):
            self.currency = symbol_data.get("currency")
            changed = True
        if self.description != symbol_data.get("description"):
            self.description = symbol_data.get("description")
            changed = True
        if self.displaySymbol != symbol_data.get("displaySymbol"):
            self.displaySymbol = symbol_data.get("displaySymbol")
            changed = True
        if self.figi != symbol_data.get("figi"):
            self.figi = symbol_data.get("figi")
            changed = True
        if self.mic != symbol_data.get("mic"):
            self.mic = symbol_data.get("mic")
            changed = True
        if self.shareClassFIGI != symbol_data.get("shareClassFIGI"):
            self.shareClassFIGI = symbol_data.get("shareClassFIGI")
            changed = True
        if self.symbol != symbol_data.get("symbol"):
            self.symbol = symbol_data.get("symbol")
            changed = True
        if self.symbol2 != symbol_data.get("symbol2"):
            self.symbol2 = symbol_data.get("symbol2")
            changed = True
        if self.type != symbol_data.get("type"):
            self.type = symbol_data.get("type")
            changed = True
        
        if self.FinnHubSymbol == 0:
            self.FinnHubSymbol = 1
            changed = 1

        if changed:
            self.data_date = datetime.now()
            self.hash = hashlib.md5(''.join(str(field) for field in self.get_hash_data()).encode('utf-8')).hexdigest()
        


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
        self.FinnHubSymbol = 1

        # Symbol itself may not be unique across exchanges
        # Combination of symbol and mic should be a unique identifier
        self._uid = ''.join((self.symbol,self.mic))

        self.hash = hashlib.md5(''.join(str(field) for field in self.get_hash_data()).encode('utf-8')).hexdigest()

        self._exists : bool = self._uid in self.__class__._id_map
        self._needs_update : bool = False

        if not self.exists():
            logger.debug("Symbol %s for exchange %s not found in DB. Persisting" ,self.symbol, self.mic)
            #self.__class__._id_map[self._uid] = id_seq.next_value()
            self.__class__._id_map[self._uid] = get_next_id()
        elif self.hash not in self.__class__._hashes:
            logger.debug("Symbol %s for exchange %s needs updating" ,self.symbol, self.mic)
            self._needs_update = True
            

        self.id = self.__class__._id_map.get(self._uid)
        #logger.warn("id value: %s" % (self.id))
        self.__class__._hashes.add(self.hash)
        #self.__class__._date_map[self._uid] = self.data_date


    def exists(self) -> bool:
        return self._exists
    
    def needs_update(self) -> bool:
        return self._needs_update

    def get_hash_data(self) -> Tuple[Any]:
        return (self.currency,self.description,self.displaySymbol,self.figi,self.mic,self.shareClassFIGI,self.symbol,self.symbol2,self.type)
    
    #def get_persist_data(self) -> Tuple[Any]:
    #    return self.get_hash_data() + (self.finnhub_symbol, self.hash, self.data_date, self.id)
    
@cacheinit
class Exchange(Base):
    __tablename__ = "Exchange"
    #id: Mapped[int] = mapped_column(id_seq,primary_key=True)
    id: Mapped[int] = mapped_column(Integer,primary_key=True)
    mic: Mapped[str] = mapped_column(String(4))
    operating_mic: Mapped[str] = mapped_column(String(4))
    oprt_sgmt: Mapped[str] = mapped_column(String(4))
    market_name: Mapped[str] = mapped_column(String(255,collation='utf8mb4_unicode_520_ci'))
    legal_entity_name: Mapped[Optional[str]] = mapped_column(String(255,collation='utf8mb4_unicode_520_ci'))
    lei: Mapped[Optional[str]] = mapped_column(String(20))
    market_category_code: Mapped[Optional[str]] = mapped_column(String(4))
    acronym: Mapped[Optional[str]] = mapped_column(String(255))
    iso_country_code: Mapped[Optional[str]] = mapped_column(String(2))
    city: Mapped[Optional[str]] = mapped_column(String(255))
    website: Mapped[Optional[str]] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(255))
    creation_date: Mapped[date] = mapped_column(Date)
    last_update_date: Mapped[Optional[date]] = mapped_column(Date)
    last_validation_date: Mapped[Optional[date]] = mapped_column(Date)
    expiry_date: Mapped[Optional[date]] = mapped_column(Date)
    comments: Mapped[Optional[str]] = mapped_column(String(255,collation='utf8mb4_unicode_520_ci'))
    hash: Mapped[str] = mapped_column(String(50))
    update_time: Mapped[datetime] = mapped_column(DateTime,server_default=func.CURRENT_TIMESTAMP())
    update_count: Mapped[int] = mapped_column(Integer,server_default=text("0"))

    __table_args__ = (
        UniqueConstraint("mic", name="ux_Exchange_mic"),
    )

    cache_stmt = select(id,mic,hash,update_time)
    #_cache_sql = "SELECT id,mic,hash,update_time FROM Exchange"
    #_insert_sql = "INSERT INTO Exchange(mic,operating_mic,oprt_sgmt,market_name,legal_entity_name,lei,market_category_code,acronym,iso_country_code,city,website,status,creation_date,last_update_date,last_validation_date,expiry_date,comments,hash,update_time,id) VALUES(" + ','.join(["%s"]*20) + ')'
    #_update_sql = "UPDATE Exchange SET mic=%s,operating_mic=%s,oprt_sgmt=%s,market_name=%s,legal_entity_name=%s,lei=%s,market_category_code=%s,acronym=%s,iso_country_code=%s,city=%s,website=%s,status=%s,creation_date=%s,last_update_date=%s,last_validation_date=%s,expiry_date=%s,comments=%s,hash=%s,update_time=%s,update_count=update_count+1 WHERE id=%s"


    #@classmethod
    #def cache_init(cls):
    #    pass

    @classmethod
    def parse_exchanges(cls, exchange_data: pd.DataFrame) -> Tuple[Dict[int,pd.Series],List['Exchange']]:
        #exchanges = []
        existing_data: Dict[int,pd.Series] = {}
        new_exchanges: List["Exchange"] = []
        for _, row in exchange_data.iterrows():
            mic: str = row['MIC']
            id = cls._id_map.get(mic)
            if id:
                existing_data[id] = row
            else:
                new_exchanges.append(Exchange(row))
        return (existing_data,new_exchanges)
    
    def compare(self, exchange_data: pd.Series):
        changed: bool = False

        if self.operating_mic != exchange_data['OPERATING MIC']:
            self.operating_mic = exchange_data['OPERATING MIC']
            changed = True
        if self.oprt_sgmt != exchange_data['OPRT/SGMT']:
            self.oprt_sgmt = exchange_data['OPRT/SGMT']
            changed = True
        if self.market_name != exchange_data['MARKET NAME-INSTITUTION DESCRIPTION']:
            self.market_name = exchange_data['MARKET NAME-INSTITUTION DESCRIPTION']
            changed = True
        if self.legal_entity_name != exchange_data['LEGAL ENTITY NAME']:
            self.legal_entity_name = exchange_data['LEGAL ENTITY NAME']
            changed = True
        if self.lei != exchange_data['LEI']:
            self.lei = exchange_data['LEI']
            changed = True
        if self.market_category_code != exchange_data['MARKET CATEGORY CODE']:
            self.market_category_code = exchange_data['MARKET CATEGORY CODE']
            changed = True
        if self.acronym != exchange_data['ACRONYM']:
            self.acronym = exchange_data['ACRONYM']
            changed = True
        if self.iso_country_code != exchange_data['ISO COUNTRY CODE (ISO 3166)']:
            self.iso_country_code = exchange_data['ISO COUNTRY CODE (ISO 3166)']
            changed = True
        if self.city != exchange_data['CITY']:
            self.city = exchange_data['CITY']
            changed = True
        if self.website != exchange_data['WEBSITE']:
            self.website = exchange_data['WEBSITE']
            changed = True
        if self.status != exchange_data['STATUS']:
            self.status = exchange_data['STATUS']
            changed = True

        creation_date: date = date.fromisoformat(str(exchange_data['CREATION DATE'])) if exchange_data['CREATION DATE'] else None
        if self.creation_date != creation_date:
            self.creation_date = creation_date
            changed = True
        last_update_date: date = date.fromisoformat(str(exchange_data['LAST UPDATE DATE'])) if exchange_data['LAST UPDATE DATE'] else None
        if self.last_update_date != last_update_date:
            self.last_update_date = last_update_date
            changed = True
        last_validation_date: date = date.fromisoformat(str(exchange_data['LAST VALIDATION DATE'])) if exchange_data['LAST VALIDATION DATE'] else None
        if self.last_validation_date != last_validation_date:
            self.last_validation_date = last_validation_date
            changed = True
        expiry_date: date = date.fromisoformat(str(exchange_data['EXPIRY DATE'])) if exchange_data['EXPIRY DATE'] else None
        if self.expiry_date != expiry_date:
            self.expiry_date = expiry_date
            changed = True

        if self.comments != exchange_data['COMMENTS']:
            self.comments = exchange_data['COMMENTS']
            changed = True

        if changed:
            self.data_date = datetime.now()
            self.hash = hashlib.md5(''.join(str(field) for field in self.get_hash_data()).encode('utf-8')).hexdigest()
            logger.debug('Updating Exchange: %s', self.mic)

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

        self.hash = hashlib.md5(''.join(str(field) for field in self.get_hash_data()).encode('utf-8')).hexdigest()

        self._exists : bool = self.mic in self.__class__._id_map
        self._needs_update : bool = False

        if not self.exists():
            logger.debug("Exchange %s not found in DB. Persisting" , self.mic)
            #self.__class__._id_map[self.mic] = id_seq.next_value()
            self.__class__._id_map[self.mic] = get_next_id()
        elif self.hash not in self.__class__._hashes:
            logger.debug("Exchange %s needs updating" , self.mic)
            self._needs_update = True
            

        self.id = self.__class__._id_map.get(self.mic)
        self.__class__._hashes.add(self.hash)

    def exists(self) -> bool:
        return self._exists
    
    def needs_update(self) -> bool:
        return self._needs_update

    def get_hash_data(self) -> Tuple[Any]:
        return (self.mic,self.operating_mic,self.oprt_sgmt,self.market_name,self.legal_entity_name,self.lei,self.market_category_code,self.acronym,self.iso_country_code,self.city,self.website,self.status,self.creation_date,self.last_update_date,self.last_validation_date,self.expiry_date,self.comments)
    
    #def get_persist_data(self) -> Tuple[Any]:
    #    return self.get_hash_data() + (self.hash, self.data_date, self.id)
    
class FinnHubQuote(Base):
    __tablename__ = "FinnHubQuote"
    #id: Mapped[int] = mapped_column(id_seq,primary_key=True)
    id: Mapped[int] = mapped_column(Integer,primary_key=True)
    symbol_key: Mapped[int] = mapped_column(Integer)
    current_price: Mapped[float] = mapped_column(Float)
    day_change: Mapped[Optional[float]] = mapped_column(Float)
    percent_change: Mapped[Optional[float]] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    open: Mapped[float] = mapped_column(Float)
    previous_close: Mapped[float] = mapped_column(Float)
    quote_time: Mapped[datetime] = mapped_column(DateTime)

    symbol: Mapped["StockSymbol"] = relationship(back_populates="quotes", 
                                                 foreign_keys=[symbol_key], 
                                                 primaryjoin="FinnHubQuote.symbol_key == StockSymbol.id")

    __table_args__ = (
        UniqueConstraint("symbol_key","quote_time", name="ux_quote_symbol_time"),
    )
    #_insert_sql = "INSERT INTO FinnHubQuote(symbol_key,current_price,day_change,percent_change,high,low,open,previous_close,quote_time,id) VALUES(" + ','.join(["%s"]*10) + ')'
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
        #self.id = id_seq.next_value()
        self.id = get_next_id()

    #def get_persist_data(self) -> Tuple[Any]:
    #    return (self.symbol_key,self.current_price, self.day_change, self.percent_change, self.high, self.low, self.open, self.previous_close, self.quote_time, self.id)


Base.metadata.create_all(engine)
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

        self.hash = hashlib.md5(''.join(str(field) for field in self.get_hash_data()).encode('utf-8')).hexdigest()

        self._exists : bool = self._uid in self.__class__._id_map
        self._needs_update : bool = False

        if not self.exists():
            logger.debug("Profile for symbol %s and exchange %s not found in DB. Persisting" ,self.ticker, self.exchange)
            self.__class__._id_map[self._uid] = DBConnection.get_next_id()
        elif self.hash not in self.__class__._hashes:
            logger.debug("Profile for symbol %s and exchange %s needs updating" ,self.ticker, self.exchange)
            self._needs_update = True
            

        self.id = self.__class__._id_map.get(self._uid)
        self.__class__._hashes.add(self.hash)
        #self.__class__._date_map[self._uid] = self.data_date


    def exists(self) -> bool:
        return self._exists
    
    def needs_update(self) -> bool:
        return self._needs_update

    def get_hash_data(self) -> Tuple[Any]:
        return (self.currency,self.description,self.displaySymbol,self.figi,self.mic,self.shareClassFIGI,self.symbol,self.symbol2,self.type)
    
    def get_persist_data(self) -> Tuple[Any]:
        return self.get_hash_data() + (self.exhange_key, self.symbol_key, self.hash, self.data_date, self.id) """