from duckberg import DuckBerg

MINIO_URI = "http://localhost:9000/"
MINIO_USER = "admin"
MINIO_PASSWORD = "password"

catalog_config: dict[str, str] = {
    "type": "rest",
    "uri": "http://localhost:8181/",
    "credentials": "admin:password",
    "s3.endpoint": MINIO_URI,
    "s3.access-key-id": MINIO_USER,
    "s3.secret-access-key": MINIO_PASSWORD,
}

catalog_name = "warehouse"

db = DuckBerg(catalog_name=catalog_name, catalog_config=catalog_config)

tables = db.list_tables()

assert len(tables) == 1

partitions = db.list_partitions(table="nyc.taxis")

assert len(tables) == 1

query: str = "SELECT * FROM 'nyc.taxis' WHERE trip_distance > 40 ORDER BY tolls_amount DESC"

df = db.select(table="nyc.taxis", partition_filter="payment_type = 1", sql=query)
