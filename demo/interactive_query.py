from duckberg import DuckBerg
from pyiceberg.expressions import EqualTo

MINIO_URI = "http://localhost:9000/"
MINIO_USER = "admin"
MINIO_PASSWORD = "password"

catalog_type: dict[str, str] = {
  "type": "rest",
  "uri": "http://localhost:8181/",
  "credentials": "admin:password",
  "s3.endpoint": MINIO_URI,
  "s3.access-key-id": MINIO_USER,
  "s3.secret-access-key": MINIO_PASSWORD
}

catalog_name = "warehouse"
tables = ["nyc.taxis"]

duckberg = DuckBerg(tables=tables,
                                 catalog_name=catalog_name,
                                 catalog_type=catalog_type)

"""
Perform select from table in catalog and do some basic filtering. Iceberg filtering is done
using pyiceberg, SQL filtering is done using DuckDB.
"""
print("Type SQL query and hit enter or type <q> to exit ...")

# Example query for this dataset: 
# SELECT * FROM 'nyc.taxis' WHERE trip_distance > 40 ORDER BY tolls_amount DESC
# SELECT * FROM 'nyc.taxis' WHERE trip_distance > 40 ORDER BY trip_distance 
# SELECT * FROM 'nyc.taxis' WHERE airport_fee > 0 ORDER BY trip_distance
query = input()
while query != 'q' and query != '<q>':
  res = duckberg.select("nyc.taxis", iceberg_filter=EqualTo("payment_type", 1), sql=query)
  print(res.read_pandas()) # this line requires pandas

  print("Type SQL query and hit enter or type <q> to exit ...")
  query = input()