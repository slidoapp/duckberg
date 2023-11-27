"""Module containing services needed for executing queries with Duckdb + Iceberg."""

from typing import Optional

from duckdb import DuckDBPyConnection
from pyarrow.lib import RecordBatchReader
from pyiceberg.catalog import Catalog, load_catalog, load_rest
from pyiceberg.table import Table

BATCH_SIZE_ROWS = 1024
DEFAULT_MEM_LIMIT = "1GB"
DEFAULT_DB_THREAD_LIMIT = 1
DEFAULT_CATALOG_NAME = "default"


class DuckBerg:
    """Iceberg + DuckDB query executor for exports in datapi"""

    def __init__(
        self,
        catalog_name: str,
        catalog_config: dict[str, str],
        db_thread_limit: Optional[int] = DEFAULT_DB_THREAD_LIMIT,
        db_mem_limit: Optional[str] = DEFAULT_MEM_LIMIT,
        batch_size_rows: Optional[int] = BATCH_SIZE_ROWS,
    ):
        self.db_thread_limit = db_thread_limit
        self.db_mem_limit = db_mem_limit
        self.batch_size_rows = batch_size_rows

        self.__get_tables(catalog_config, catalog_name)

    def __get_tables(self, catalog_config, catalog_name):
        tables = {}
        catalog: Catalog = load_catalog(catalog_name, **catalog_config)
        rest = load_rest(catalog_name, catalog_config)
        namespaces = rest.list_namespaces()
        for n in namespaces:
            tables_names = rest.list_tables(n)
            self.tables: dict[str, Table] = {".".join(t): catalog.load_table(t) for t in tables_names}
        return tables

    def list_tables(self):
        return list(self.tables.keys())

    def list_partitions(self, table: str):
        t = self.tables[table]

        if t.spec().is_unpartitioned():
            return None

        partition_cols_ids = [p["source-id"] for p in t.spec().model_dump()["fields"]]
        col_names = [c["name"] for c in t.schema().model_dump()["fields"] if c["id"] in partition_cols_ids]

        return col_names

    def select(self, table: str, sql: str, partition_filter: str, sql_params: [str] = None) -> RecordBatchReader:
        db_conn: DuckDBPyConnection = self.tables[table].scan(row_filter=partition_filter).to_duckdb(table_name=table)

        db_conn.execute(f"SET memory_limit='{self.db_mem_limit}'")
        db_conn.execute(f"SET threads TO {self.db_thread_limit}")

        if sql_params is None:
            return db_conn.execute(sql).fetch_record_batch(self.batch_size_rows).read_pandas()
        else:
            return db_conn.execute(sql, parameters=sql_params).fetch_record_batch(self.batch_size_rows).read_pandas()
