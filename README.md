### Duckberg

Combination of Apache Iceberg's `pyiceberg` and `duckdb` to perform fast queries on iceberg data.

Example usage (for more, have a look into: /demo):

- First setup catalog for duckdberg
```python
from duckberg import DuckBerg
from pyiceberg.expressions import EqualTo

catalog_type: dict[str, str] = {
  # your catalog setup
}

catalog_name = "warehouse"
tables = ["nyc.taxis"]

duckberg = DuckBerg(tables=tables, catalog_name=catalog_name, catalog_type=catalog_type)
```

- Then perform `select` queries

```python
query: str = "SELECT * FROM 'nyc.taxis' WHERE trip_distance > 40 ORDER BY tolls_amount DESC"
res = duckberg.select("nyc.taxis", iceberg_filter=EqualTo("payment_type", 1), sql=query)
```