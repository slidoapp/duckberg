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

assert(len(tables) == 1)

# New way of quering data without partition filter
query: str = "SELECT count(*) FROM (SELECT * FROM 'nyc.taxis' WHERE trip_distance > 40 ORDER BY tolls_amount DESC)"
df = db.select(sql=query).read_pandas()
assert(df['count_star()'][0] == 2614)

# New way of quering data
query: str = "SELECT count(*) FROM (SELECT * FROM 'nyc.taxis' WHERE payment_type = 1 AND trip_distance > 40 ORDER BY tolls_amount DESC)"
df = db.select(sql=query).read_pandas()
assert(df['count_star()'][0] == 1673)

# Old way of quering data
query: str = "SELECT count(*) FROM (SELECT * FROM 'nyc.taxis' WHERE payment_type = 1 AND trip_distance > 40 ORDER BY tolls_amount DESC)"
df = db.select(sql=query, table="nyc.taxis", partition_filter="payment_type = 1").read_pandas()
assert(df['count_star()'][0] == 1673)
