"""Module containing services needed for executing queries with Duckdb + Iceberg."""

from duckdb import DuckDBPyConnection
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import LiteralPredicate
from pyiceberg.table import Table
from pyarrow.lib import RecordBatchReader

BATCH_SIZE_ROWS = 1024
DEFAULT_MEM_LIMIT = "1GB"
DEFAULT_DB_THREAD_LIMIT = 1
DEFAULT_CATALOG_NAME="default"
DEFAULT_CATALOG_TYPE="glue"


class DuckBerg:
    """Iceberg + DuckDB query executor for exports in datapi"""

    def __init__(self, tables: [str],
                catalog_name: str = DEFAULT_CATALOG_NAME,
                catalog_type: dict[str, str] = None,
                db_thread_limit: int = DEFAULT_DB_THREAD_LIMIT,
                db_mem_limit: str = DEFAULT_MEM_LIMIT,
                batch_size_rows: int = BATCH_SIZE_ROWS):

      if catalog_type is None:
          catalog_type: dict[str, str] = {"type": DEFAULT_CATALOG_TYPE}

      catalog: Catalog = load_catalog(catalog_name, **catalog_type)

      self.db_thread_limit = db_thread_limit
      self.db_mem_limit = db_mem_limit
      self.batch_size_rows = batch_size_rows

      self.tables: dict[str, Table] = dict(
          map(lambda tn: (tn, catalog.load_table(tn)), tables)
      )

    def select(self, table: str, iceberg_filter: LiteralPredicate, sql: str, sql_params: [str] = None) -> RecordBatchReader:
      db_conn: DuckDBPyConnection = self.tables[table] \
                                        .scan(row_filter=iceberg_filter) \
                                        .to_duckdb(table_name=table)
      print("dwada")
      db_conn.execute(f"SET memory_limit='{self.db_mem_limit}'")
      db_conn.execute(f"SET threads TO {self.db_thread_limit}")
      
      if sql_params is None:
        return db_conn.execute(sql).fetch_record_batch(self.batch_size_rows)
      else:
        return db_conn.execute(sql, parameters=sql_params).fetch_record_batch(self.batch_size_rows)