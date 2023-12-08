import os
import logging
import logging.config
from YahooData import backfill_data

WORKING_DIR = os.path.dirname(os.path.realpath(__file__))
logging.config.fileConfig(WORKING_DIR+os.sep+'logging.conf')

backfill_data()