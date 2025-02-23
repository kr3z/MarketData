import os
import logging
#import logging.config
import traceback
import numpy as np
import pandas as pd
import urllib.request
from typing import cast, Set

from sqlalchemy import select

from FinnHubClasses import Exchange
#from DB import DBConnection

from Base import Session

WORKING_DIR = os.path.dirname(os.path.realpath(__file__))
#logging.config.fileConfig(WORKING_DIR+os.sep+'logging.conf')
logger = logging.getLogger('MIC')
ISO10383_URL = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"
ISO10383_PATH = WORKING_DIR+os.path.sep+'stage'+os.path.sep+'ISO10383_MIC.csv'

def download_iso10383() -> str:
    logger.debug("Downloading latest MIC data to : %s", ISO10383_PATH)
    path, headers = urllib.request.urlretrieve(ISO10383_URL, ISO10383_PATH)
    logger.debug("Downloaded to: %s", path)
    #logger.trace(headers)
    return path

def import_iso10383(filename: str) -> None:
    logger.debug("Importing data from: %s", filename)
    csvFile = pd.read_csv(filename, encoding='latin-1')
    csvFile = csvFile.replace({np.nan: None})
    mic_df = csvFile.set_index('MIC')

    mics = mic_df.index.to_list()
    existing_exchanges = cast(Set[Exchange],Exchange.get_all_from_cache(mics, 'mic'))
    existing_mics = [existing_exchange.mic for existing_exchange in existing_exchanges]
    new_mics = [mic for mic in mics if mic not in existing_mics]

    logger.debug(f"Found {len(new_mics)} New Exchanges")
    logger.debug(f"Comparing {len(existing_mics)} Existing Exchanges")

    with Session() as session:
        for exchange in existing_exchanges:
            if exchange.compare(mic_df.loc[exchange.mic]):
                session.merge(exchange)
        session.flush()

        new_exchanges = Exchange.parse_data(mic_df.loc[new_mics])
        session.add_all(new_exchanges)
        session.flush()

        """no_longer_finnhub_symbols = session.scalars(select(StockSymbol).where(StockSymbol.finnhub_symbol==1, StockSymbol.id.not_in(uids))).all()
        for symbol in no_longer_finnhub_symbols:
            symbol.finnhub_symbol = 0"""
         
        session.commit()
    


    """ existing_data, new_exchanges = Exchange.parse_exchanges(csvFile.astype(object).where(pd.notnull(csvFile),None))
    existing_ids =  set(existing_data.keys())
    with Session() as session:
        logger.debug("Comparing %d Existing Exchanges", len(existing_ids))
        exisiting_exchanges = session.scalars(select(Exchange).where(Exchange.id.in_(existing_ids))).all()
        for existing_exchange in exisiting_exchanges:
                existing_exchange.compare(existing_data[existing_exchange.id])

        logger.debug("Importing %d New Exchanges", len(new_exchanges))
        session.add_all(new_exchanges)
        session.commit() """

    """ logger.debug("Parsed %d Exchanges", len(exchanges))
    new_ex = []
    update_ex = []
    for ex in exchanges:
        if not ex.exists():
            new_ex.append(ex.get_persist_data())
        elif ex.needs_update():
            update_ex.append(ex.get_persist_data())
    #csvFile.astype(object).where(pd.notnull(csvFile),None).to_numpy().tolist()

    conn = None
    try:
        conn = DBConnection.getConnection()
        logger.debug("Importing %d new exchanges",len(new_ex))
        conn.executemany(Exchange._insert_sql,new_ex)
        conn.commit()
        
        logger.debug("Updating %d existing exchanges",len(update_ex))
        conn.executemany(Exchange._insert_sql,update_ex)
        conn.commit()
    except Exception as error:
        logger.error(error)
        traceback.print_exc()
        if conn is not None:
            conn.rollback() """

#filename = download_iso10383()
#import_iso10383(filename)
import_iso10383(ISO10383_PATH)
