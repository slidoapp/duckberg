from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.catalog.sql import Catalog
from pyiceberg.exceptions import NoSuchTableError
import logging
import time

DATA_FILE = "data/yellow_tripdata_2022-12.parquet"
MINIO_URI = "http://minio:9000/"
MINIO_USER = "admin"
MINIO_PASSWORD = "password"


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

logging.info("Loading catalog from REST ...")
catalog_type: dict[str, str] = {
  "type": "rest",
  "uri": "http://iceberg-rest:8181/",
  "credentials": "admin:password",
  "s3.endpoint": MINIO_URI,
  "s3.access-key-id": MINIO_USER,
  "s3.secret-access-key": MINIO_PASSWORD
}

catalog: Catalog = load_catalog("warehouse", **catalog_type)

namespaces = catalog.list_namespaces()
logging.info(f"Namespaces: {namespaces}")

while len(namespaces) == 0:
  time.sleep(1.0)
  logging.info("Waiting for some namespaces to appear ... ")
  logging.info(f"Namespaces: {namespaces}")
  namespaces = catalog.list_namespaces()

tables = catalog.list_tables(namespaces[0])
logging.info(f"Tables in warehouse: {tables}")

found = False
taxis_table = None
while not found:
  try:
    taxis_table = catalog.load_table("nyc.taxis")
    found = True
  except NoSuchTableError:
    logging.info("Waiting for table to be populated ...")
    time.sleep(1.0)

logging.info(f"warehouse.taxis location: {taxis_table.metadata}")

scan = taxis_table.scan(
    row_filter=EqualTo("payment_type", 1)
)

files = [task.file.file_path for task in scan.plan_files()]
logging.info(f"Files containing rows with payment_type=1 {files}")