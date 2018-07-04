import os


SOURCE = os.environ.get("SOURCE", "")
TARGET = os.environ.get("TARGET", "")
AWAIT = int(os.environ.get("AWAIT", 5))
DB_MAP = os.environ.get("DB_MAP", "")
TS_FILE = os.environ.get("TS_FILE", "last_sync_ts")
CHUNK_CONFS = os.environ.get("CHUNK_CONFS", None) 

