"""Module containing services needed for executing queries with Duckdb + Iceberg."""

from typing import Optional

import duckdb
from pyarrow.lib import RecordBatchReader
from pyiceberg.catalog import Catalog, load_catalog, load_rest
from pyiceberg.expressions import AlwaysTrue

from duckberg.exceptions import TableNotInCatalogException
from duckberg.sqlparser import DuckBergSQLParser
from duckberg.table import DuckBergTable, TableWithAlias

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
        duckdb_connection: duckdb.DuckDBPyConnection = None,
        db_thread_limit: Optional[int] = DEFAULT_DB_THREAD_LIMIT,
        db_mem_limit: Optional[str] = DEFAULT_MEM_LIMIT,
        batch_size_rows: Optional[int] = BATCH_SIZE_ROWS,
    ):
        self.db_thread_limit = db_thread_limit
        self.db_mem_limit = db_mem_limit
        self.batch_size_rows = batch_size_rows
        self.duckdb_connection = duckdb_connection

        if self.duckdb_connection == None:
            self.duckdb_connection = duckdb.connect()
            self.init_duckdb()

        self.sql_parser = DuckBergSQLParser()
        self.tables: dict[str, DuckBergTable] = {}

        self.init_duckdb()
        self.__get_tables(catalog_config, catalog_name)

    def init_duckdb(self):
        self.duckdb_connection.execute(f"SET memory_limit='{self.db_mem_limit}'")
        self.duckdb_connection.execute(f"SET threads TO {self.db_thread_limit}")

    def __get_tables(self, catalog_config, catalog_name):
        tables = {}
        catalog: Catalog = load_catalog(catalog_name, **catalog_config)
        rest = load_rest(catalog_name, catalog_config)
        namespaces = rest.list_namespaces()
        for n in namespaces:
            tables_names = rest.list_tables(n)
            self.tables: dict[str, DuckBergTable] = {
                ".".join(t): DuckBergTable.from_pyiceberg_table(catalog.load_table(t)) for t in tables_names
            }
        return tables

    def list_tables(self):
        return list(self.tables.keys())

    def list_partitions(self, table: str):
        t = self.tables[table]

        if t.partitions == None:
            t.precomp_partitions()

        return t.partitions

    def select(
        self, sql: str, table: str = None, partition_filter: str = None, sql_params: [str] = None
    ) -> RecordBatchReader:
        if table is not None and partition_filter is not None:
            return self._select_old(sql, table, partition_filter, sql_params)

        parsed_sql = self.sql_parser.parse_first_query(sql)
        extracted_tables = self.sql_parser.extract_tables(parsed_sql)

        table: TableWithAlias
        for table in extracted_tables:
            table_name = table.table_name

            if table_name not in self.tables:
                raise TableNotInCatalogException

            row_filter = AlwaysTrue()
            if table.comparisons is not None:
                row_filter = table.comparisons

            table_data_scan_as_arrow = self.tables[table_name].scan(row_filter=row_filter).to_arrow()
            self.duckdb_connection.register(table_name, table_data_scan_as_arrow)

        if sql_params is None:
            return self.duckdb_connection.execute(sql).fetch_record_batch(self.batch_size_rows)
        else:
            return self.duckdb_connection.execute(sql, parameters=sql_params).fetch_record_batch(self.batch_size_rows)

    def _select_old(self, sql: str, table: str, partition_filter: str, sql_params: [str] = None):
        table_data_scan_as_arrow = self.tables[table].scan(row_filter=partition_filter).to_arrow()
        self.duckdb_connection.register(table, table_data_scan_as_arrow)

        if sql_params is None:
            return self.duckdb_connection.execute(sql).fetch_record_batch(self.batch_size_rows)
        else:
            return self.duckdb_connection.execute(sql, parameters=sql_params).fetch_record_batch(self.batch_size_rows)
