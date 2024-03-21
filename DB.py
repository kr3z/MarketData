import configparser
import mysql.connector
import traceback
import os
import threading
import logging

from sqlalchemy import create_engine, URL

logger = logging.getLogger('DB')

WORKING_DIR = os.path.dirname(os.path.realpath(__file__))

config = configparser.ConfigParser()
config.read(WORKING_DIR+os.sep+'config.properties')

db_host=config.get("db", "db_host")
db_name=config.get("db", "db_name")
db_user=config.get("db", "user")
db_password=config.get("db", "password")

#engine = create_engine("mysql+mysqldb://@localhost/test", echo=True)

sqlalchemy_url = URL.create(
    "mysql+mysqldb",
    username=db_user,
    password=db_password,
    host=db_host,
    database=db_name,
)

#engine = create_engine(sqlalchemy_url, pool_pre_ping=True, echo=True)
#connection = engine.connect()

class DBConnection():
    _open_connections = []
    _id_pool = []
    _owned_connections = {}

    def __init__(self):
        self.conn = mysql.connector.connect(host=db_host,
                                   database=db_name,
                                   user=db_user,
                                   password=db_password)

        self.conn.autocommit = False
        self.conn.sql_mode = 'TRADITIONAL,NO_ENGINE_SUBSTITUTION'
        self.cursor = self.conn.cursor()

        DBConnection._open_connections.append(self.conn)


    def close(self) -> None:
         if self.conn is not None:
            if self.cursor is not None:
                self.cursor.close()
                self.curosr = None
            DBConnection._open_connections.remove(self.conn)
            if threading.get_ident() in DBConnection._owned_connections:
                del DBConnection._owned_connections[threading.get_ident()]
            self.conn.close()
            self.conn = None

    def commit(self):
        self.conn.commit()
    def rollback(self):
        self.conn.rollback()

    def start_transaction(self) -> None:
        if not self.conn.in_transaction:
            self.conn.start_transaction(consistent_snapshot=True, isolation_level='READ COMMITTED', readonly=False)

    def in_transaction(self) -> bool:
        return self.conn.in_transaction
    
    def is_connected(self) -> bool:
        return self.conn.is_connected()

    def execute(self,query: str,binds: list = None) -> list:
        res = None
        try:
            if not self.cursor:
                self.cursor = self.conn.cursor()
            self.cursor.execute(query,binds)
            res = self.cursor.fetchall()
        except Exception as error:
            logger.error(error)
            traceback.print_exc()
            if self.conn:
                self.rollback()
        return res
    
    # Used for bulk inserts
    def executemany(self,query: str,binds: list = None) -> list:
        res = None
        try:
            if not self.cursor:
                self.cursor = self.conn.cursor()
            self.cursor.executemany(query,binds)
            res = self.cursor.fetchall()
        except Exception as error:
            logger.error(error)
            traceback.print_exc()
            if self.conn:
                self.rollback()
        return res
    
    # This is intended to be used for non-locking SELECT queries
    # In mariadb, selects will hold a table metadata lock which we don't need to
    # continue holding if the query is not part of a larger transaction.
    # This method checks if the connection is already in a transaction and then calls execute
    # Then if the connection was not already in a transaction, it commits to release the metadata lock
    def executeQuery(self,query: str,binds: list = None) -> list:
        tran = self.in_transaction()
        res = self.execute(query,binds)
        if not tran:
            self.conn.commit()
        return res
    
    @classmethod
    def getConnection(cls) -> 'DBConnection':
        dbconn = cls._owned_connections.get(threading.get_ident())
        if dbconn is None or not dbconn.is_connected():
            if dbconn:
                dbconn.close()
            dbconn = DBConnection()
            cls._owned_connections[threading.get_ident()] = dbconn

        return dbconn
    
    @classmethod
    def get_next_id(cls) -> int:
        if(len(cls._id_pool) == 0):
            logger.info("id pool empty, querying for more")
            cls.fill_pool()
        return cls._id_pool.pop(0)
    
    @classmethod
    def get_ids(cls, nIds: int) -> list:
        while len(cls._id_pool)<nIds:
            cls.fill_pool()
        ret_ids = cls._id_pool[0:nIds]
        del cls._id_pool[0:nIds]
        return ret_ids
    
    @classmethod
    def fill_pool(cls) -> None:
        conn = cls.getConnection()
        res = conn.executeQuery("SELECT NEXTVAL(id_seq),increment from id_seq")
        #res = cls.singleQuery("SELECT NEXTVAL(id_seq),increment from id_seq")
        next_val = res[0][0]
        increment = res[0][1]
        logger.debug("Adding values %d to %d to id pool"  % (next_val,next_val+increment))
        cls._id_pool.extend(range(next_val,next_val+increment))