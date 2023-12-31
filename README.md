# duckberg

<p align="center">
  <img src="https://raw.githubusercontent.com/slidoapp/duckberg/main/static/images/duckberg.png" /> <br />
  <strong>DuckBerg</strong> <br />
  <em>query your iceberg data easily and efficiently</em>
</p>


[![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch) 
[![PyPI - Version](https://img.shields.io/pypi/v/duckberg.svg)](https://pypi.org/project/duckberg)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/duckberg.svg)](https://pypi.org/project/duckberg)
[![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![code style - Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-green.svg)

-----

**Table of Contents**

- [About](#about)
- [Installation](#installation)
- [Features](#features)
- [Playground](#playground)
- [Development](#development)
- [License](#license)

## About
Duckberg is a Python package that synthesizes the power of PyIceberg and DuckDb. PyIceberg enables efficient 
interaction with Apache Iceberg, a format for handling large datasets, while DuckDb offers swift in-memory data 
analysis. When combined, these tools create Duckberg, which simplifies the querying process for large Iceberg 
datasets stored on blob storage with a user-friendly Pythonic approach.

The underlying principle of the Duckberg Python package is to execute your SQL queries only on those data lake files 
that contain the necessary data for your results. To fully utilize the benefits of this package, it's assumed that 
your data is partitioned in a manner that suits your query and use case.

### Iceberg catalog types
Duckberg supports the same Iceberg catalogs as [PyIceberg](https://py.iceberg.apache.org/configuration/), including 
REST, SQL, Hive, Glue, and DynamoDB. These catalogs are sources of information about Iceberg datasets, tables, 
partitions, etc. Before using Duckberg, ensure that you have access to an Iceberg catalog that can be utilized.

## Installation

```console
pip install duckberg
```

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

In the latest new update we have added very crude and simple SQL parser that can extract necessary information from the SQL query without the need to specify `table` and `partition_filters`. This is the new and prefered way:

```python
query = "SELECT * FROM nyc.taxis WHERE payment_type = 1 AND trip_distance > 40 ORDER BY tolls_amount DESC"
df = db.select(sql=query).read_pandas()
```

Old way of selecting data (will get deprecated in the future):

```python
query = "SELECT * FROM nyc.taxis WHERE trip_distance > 40 ORDER BY tolls_amount DESC"
df = db.select(sql=query, table="nyc.taxis", partition_filter="payment_type = 1").read_pandas()
```

## Playground
You can run the playground environment running docker compose in the [playground](./playground)

```shell
cd playground
docker-compose up -d
```

The initial run could take additional time for jupyter docker image build. Then you can access

### Iceberg data init
Once all the containers have been initiated run the [Spark Iceberg Jupyter notebook](http://localhost:8889/notebooks/000%20Init%20Iceberg%20data.ipynb) that will
init the Iceberg data and catalog.

### Duckberg playground
Navigate to [localhost:8888](http://localhost:8888). Then select example Jupyter notebook you want to run and enjoy Duckberg!

## Development
For the development, there is recommendation to use Python 3.10. If you manage your Python versions by 
[Pyenv](https://github.com/pyenv/pyenv) use 

```bash
pyenv install 3.10.13
pyenv global 3.10.13
```

then create and activate virtual environment
```bash
python -m venv venv
source venv/bin/activate 
```

upgrade pip and install dependencies

```bash
pip install --upgrade pip
pip install .
```

then run dockers that contains Iceberg catalog and file storage containing iceberg files

```shell
cd playground
docker-compose up -d
```

init data by running [Init Jupyter notebook](http://localhost:8889/notebooks/000%20Init%20Iceberg%20data.ipynb) and
run/test Duckberg in the file `tests/duckberg-sample.py`

### Style & Formatting

Use 

```bash
hatch run lint:fmt
hatch run lint:style
```

## Building package

The Duckberg project is managed by [Hatch](https://hatch.pypa.io/latest/). Follow [Hatch docs] for an installation
or just install by command

```shell
brew install hatch
```

or 

```shell
pip install hatch
```

Increase package by
```bash
hatch version "x.x.x"
```

Build 
```bash
hatch build
```

and publish
```bash
hatch publish
```
## License

`duckberg` is distributed under the terms of the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt) license.
