import os
import logging
import logging.config
import configparser
from typing import List, Tuple, Dict, Type, Optional, Iterable, Set
from threading import RLock

from sqlalchemy.orm import DeclarativeBase, sessionmaker, mapped_column, Mapped
from sqlalchemy import create_engine, Sequence, URL, event, text, Integer, select, Column, inspect

WORKING_DIR = os.path.dirname(os.path.realpath(__file__))

config = configparser.ConfigParser()
config.read(WORKING_DIR+os.sep+'config.properties')

API_KEY = config.get("api", "api_key")

logging.config.fileConfig(WORKING_DIR+os.sep+'logging.conf')
logger = logging.getLogger('DB')


db_host=config.get("db", "db_host")
db_name=config.get("db", "db_name")
db_user=config.get("db", "user")
db_password=config.get("db", "password")

sqlalchemy_url = URL.create(
    "mysql+mysqldb",
    username=db_user,
    password=db_password,
    host=db_host,
    database=db_name,
)

class Base(DeclarativeBase):
    pass

engine = create_engine(sqlalchemy_url, pool_pre_ping=True, isolation_level="READ COMMITTED")
#engine = create_engine(sqlalchemy_url, pool_pre_ping=True, echo=True, isolation_level="READ COMMITTED")

@event.listens_for(engine, "connect", insert=True)
def connect(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("SET sql_mode = 'TRADITIONAL,NO_ENGINE_SUBSTITUTION'")

Session = sessionmaker(engine, autoflush=False, expire_on_commit=False)

id_seq = Sequence("id_seq",metadata=Base.metadata, start=1, increment=1000, cache=10)

_id_pool = []

def get_next_id() -> int:
    if(len(_id_pool) == 0):
        logger.info("id pool empty, querying for more")
        fill_pool()
    return _id_pool.pop(0)
    
def get_ids(nIds: int) -> List[int]:
    while len(_id_pool)<nIds:
        fill_pool()
    ret_ids = _id_pool[0:nIds]
    del _id_pool[0:nIds]
    return ret_ids
    
""" def fill_pool() -> None:
    #conn = cls.getConnection()
    #res = conn.executeQuery("SELECT NEXTVAL(id_seq),increment from id_seq")
    with Session() as session:
        res = session.execute(text("SELECT NEXTVAL(id_seq),increment from id_seq")).first()
        #res = cls.singleQuery("SELECT NEXTVAL(id_seq),increment from id_seq")
        next_val = res[0]
        increment = res[1]
        logger.debug("Adding values %d to %d to id pool"  % (next_val,next_val+increment))
        _id_pool.extend(range(next_val,next_val+increment)) """
    
def fill_pool() -> None:
    with Session() as session:
        res = session.execute(text("SELECT NEXTVAL(id_seq),increment from id_seq")).first()
        if res:
            next_val = res[0]
            increment = res[1]
            logger.debug("Adding values %d to %d to id pool"  % (next_val,next_val+increment))
            _id_pool.extend(range(next_val,next_val+increment))
        else:
            logger.error("Unable to fill id pool from id_seq")
            raise


class DBObjectCache:
    _cache_map: Dict[str,Dict[str|int,"CacheableDBObject"]]
    #_reverse_map: Dict[CacheableDBObject,Dict[str,str|int]]
    _cacheable_attributes: Tuple[str,...]
    _type: Type["CacheableDBObject"]
    _lock : RLock
    _hits: int
    _misses: int
    
    def __init__(self, type_: Type["CacheableDBObject"], cachable_attributes: Tuple[str,...]):
        self._lock = RLock()
        self._type = type_
        self._cacheable_attributes = cachable_attributes
        self._cache_map = {attr: {} for attr in self._cacheable_attributes}
        self._hits = 0
        self._misses = 0

    def put(self, object: "CacheableDBObject"):
        with self._lock:
            reverse_map: Dict[str,str|int] = {}
            for attr in self._cacheable_attributes:
                map = self._cache_map[attr]
                key = getattr(object, attr)
                map[key] = object
                self._cache_map[attr] = map
                reverse_map[attr] = key
            #self._reverse_map[object] = reverse_map


    def get(self, attr: str, key: str|int):
        with self._lock:
            map = self._cache_map.get(attr)
            if map is None:
                logger.error("Attribute: %s is not a cacheable attribute for type: %s", attr, self._type)
                raise
            obj = map.get(key)
            if obj:
                self._hits += 1
                #if self._hits % 1000 == 0:
                #    logger.warning("%s Cache Gets: %s", self._type, self._hits)
            else:
                self._misses += 1
                #if self._misses % 1000 == 0:
                #    logger.warning("%s Cache Miesses: %s", self._type, self._misses)
            return obj
        
    """ def remove(self, object: CacheableDBObject):
        with self._lock:
            reverse_map = self._reverse_map.get(object)
            if reverse_map:
                for attr, key in reverse_map.items():
                    del self._cache_map[attr][key]
            else:
                logger.info("Object: %s is not cached for type: %s", object, CacheableDBObject) """
    
class DBObject(Base):
    __abstract__ = True
    id: Mapped[int] = mapped_column(Integer, primary_key=True, sort_order=-1, autoincrement=False)
    #create_stamp: Mapped[datetime.datetime] = mapped_column(DateTime, sort_order=100, default=datetime.datetime.now)
    ##update_count: Mapped[int] = mapped_column(Integer, sort_order=101)
    #update_stamp: Mapped[datetime.datetime] = mapped_column(DateTime, sort_order=102, onupdate=datetime.datetime.now)

    #__mapper_args__ = {"version_id_col": update_count}

    def __init__(self):
        self.id = get_next_id()

class CacheableDBObject(DBObject):
    __abstract__ = True
    _cache: "DBObjectCache"
    _cacheable_attributes: Tuple[str, ...]
    _attribute_to_column_map: Dict[str, Column]

    @classmethod
    def get_from_cache(cls,cache_key: str|int, cache_attribute: str) -> Optional["CacheableDBObject"]:
        with cls._cache._lock:
            cache_object = cls._cache.get(cache_attribute,cache_key)
            if not cache_object:
                with Session() as session:
                    cache_object = session.scalars(select(cls).filter_by(**{cache_attribute:cache_key})).first()
                    if cache_object:
                        cls._cache.put(cache_object)

            return cache_object
        

    @classmethod
    def get_all_from_cache(cls, cache_keys: Iterable[str] | Iterable[int], cache_attribute: str) -> Set["CacheableDBObject"]:
        with cls._cache._lock:
            hits: Set["CacheableDBObject"] = set()
            misses: Set[str] | Set[int] = set()
            attribute_column = cls._attribute_to_column_map.get(cache_attribute)
            if attribute_column is None:
                logger.error(f"Attribute: {cache_attribute} is not a cacheable attribute for type: {cls.__name__}")
                logger.error(f"_cacheable_attributes: {cls._attribute_to_column_map.items()}")
                raise
            for cache_key in cache_keys:
                cache_object = cls._cache.get(cache_attribute,cache_key)
                if cache_object:
                    hits.add(cache_object)
                else:
                    misses.add(cache_key)
            with Session() as session:
                cache_objects = session.scalars(select(cls).where(attribute_column.in_(misses))).fetchall()
                for cache_object in cache_objects:
                    cls._cache.put(cache_object)
                    hits.add(cache_object)
            return hits
        
    @classmethod
    def add_to_cache(cls, object: "CacheableDBObject"):
        with cls._cache._lock:
            cls._cache.put(object)

    def __init__(self):
        super().__init__()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._cache = DBObjectCache(cls, cls._cacheable_attributes)

        ins = inspect(cls)
        for cache_att in cls._cacheable_attributes:
            logger.debug(f"initializing cacheable attribute: {cache_att} for type: {cls.__name__}")
            col = ins.columns.get(cache_att)
            if col is not None:
                cls._attribute_to_column_map[cache_att] = col
            else:
                logger.error(f"Attribute: '{cache_att}' is not a valid column for type '{cls.__name__}'")
                raise