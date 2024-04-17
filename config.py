import logging
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER') or 'gis'
DB_PWD = os.getenv('DB_PWD') or 'gis'
DB_HOST = os.getenv('DB_HOST') or '0.0.0.0'
DB_PORT = os.getenv('DB_PORT') or 5584
DB_NAME = os.getenv('DB_NAME') or 'gis'
DB_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
DB_SCHEMA = 'public'

HOST = os.getenv('HOST') or '0.0.0.0'
PORT = int(os.getenv('PORT') or 84)

LOG_FORMAT = '[%(levelname) -3s %(asctime)s] %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)