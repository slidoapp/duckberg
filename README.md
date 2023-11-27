# duckberg

<p align="center">
  <img src="https://raw.githubusercontent.com/slidoapp/duckberg/main/static/images/duckberg.png" /> <br />
  <strong>Duckberg</strong> <br />
  <em>query your data lakes easy and efficient</em>
</p>


[![PyPI - Version](https://img.shields.io/pypi/v/duckberg.svg)](https://pypi.org/project/duckberg)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/duckberg.svg)](https://pypi.org/project/duckberg)

-----

**Table of Contents**

- [About](#about)
- [Installation](#installation)
- [Examples](#examples)
- [Features](#features)
- [Development](#development)
- [License](#license)

## About
Duckberg is a Python package that synthesizes the power of PyIceberg and DuckDb. PyIceberg enables efficient 
interaction with Apache Iceberg, a format for handling large datasets, while DuckDb offers swift in-memory data 
analysis. When combined, these tools create Duckberg, which simplifies the querying process for large Iceberg 
datasets stored on blob storage. Duckberg offers high-speed data processing, memory efficiency, and a user-friendly 
Pythonic approach, making the querying of big data without an external query engine easy and efficient.

The underlying principle of the Duckberg Python package is to execute your SQL queries only on those data lake files 
that contain the necessary data for your results. To fully utilize the benefits of this package, it's assumed that 
your data is partitioned in a manner that suits your query and use case.

### Iceberg catalog types
Duckberg supports the same Iceberg catalogs as [PyIceberg]((https://py.iceberg.apache.org/configuration/)), including 
REST, SQL, Hive, Glue, and DynamoDB. These catalogs are sources of information about Iceberg datasets, tables, 
partitions, etc. Before using Duckberg, ensure that you have access to an Iceberg catalog that can be utilized.

## Installation

```console
pip install duckberg
```

## Examples
This repository contains docker compose environment that uses 
- [Spark Iceberg](https://github.com/tabular-io/docker-spark-iceberg) for Iceberg data initialisation through Spark 
- [Rest Iceberg Catalog](https://github.com/tabular-io/iceberg-rest-image) for storing Iceberg metadata 
- [Minio](https://min.io/) as an object storage that is S3 compatible. 
- Jupyter with preinstalled Duckberg to run examples and experiments

To spin up local environment use

```shell
cd examples
docker-compose up -d
```

The initial run could take additional time for jupyter docker image build

### Iceberg data init
Once all the containers have been initiated go to [Spark Iceberg Jupyter notebook](http://localhost:8889/notebooks/000%20Init%20Iceberg%20data.ipynb) and run the 
notebook to init the Iceberg data and catalog.

### Duckberg playground

Navigate to [localhost:8888](localhost:8888). Then select example Jupyter notebook you want to run and enjoy Duckberg!

## Features

### Easy initialisation
Following initialisation is using the `REST Iceberg catalog` with `Amazon S3` as a iceberg data storage.

```python
from duckberg import DuckBerg

catalog_config: dict[str, str] = {
  "type": "rest", # Iceberg catalog type 
  "uri": "http://iceberg-rest:8181/", # url for Iceberg catalog
  "credentials": "user:password", # credentials for Iceberg catalog
  "s3.endpoint": S3_ENDPOINT, # s3 
  "s3.access-key-id": S3_ACCESS_KEY_ID,
  "s3.secret-access-key": S3_SECET_KEY
}

db = DuckBerg(
     catalog_name="warehouse",
     catalog_config=catalog_config)
```

### Listing tables

```python
db.list_tables()
```

### Listing partitions for particular table

```python
db.list_partitions(table="nyc.taxis")
```

### Querying data to Pandas dataframe

```python
query = "SELECT * FROM nyc.taxis WHERE trip_distance > 40 ORDER BY tolls_amount DESC"
df = db.select(table="nyc.taxis", partition_filter="payment_type = 1", sql=query)
```

## Development

TBD ...

## License

`duckberg` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
