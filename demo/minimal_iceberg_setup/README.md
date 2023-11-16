### Iceberg minimal setup for Duckberg

Minimal example of iceberg with pyspark and NYC taxis dataset

- Start by downloading data and placing it into notebooks/data 
  - (eg. run `curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-12.parquet --output yellow_tripdata_2022-12.parquet` in notebooks/data)
- Start by running `docker compose up`
- Everything is set up
- Play with `src/main.py`
- Data is bucketed by `payment_type` into 8 buckets