import os
import logging
import logging.config
import configparser
from typing import List

from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import create_engine, Sequence, URL, event, text

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

Session = sessionmaker(engine)

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
    
def fill_pool() -> None:
    #conn = cls.getConnection()
    #res = conn.executeQuery("SELECT NEXTVAL(id_seq),increment from id_seq")
    with Session() as session:
        res = session.execute(text("SELECT NEXTVAL(id_seq),increment from id_seq")).first()
        #res = cls.singleQuery("SELECT NEXTVAL(id_seq),increment from id_seq")
        next_val = res[0]
        increment = res[1]
        logger.debug("Adding values %d to %d to id pool"  % (next_val,next_val+increment))
        _id_pool.extend(range(next_val,next_val+increment))