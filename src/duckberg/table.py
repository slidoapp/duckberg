import sqlparse
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import BooleanExpression
from pyiceberg.io import FileIO
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.typedef import Identifier


class DuckBergTable(Table):
    """
    Class for storing precomputed data for faster processing of queries
    """

    def __init__(
        self, identifier: Identifier, metadata: TableMetadata, metadata_location: str, io: FileIO, catalog: Catalog
    ) -> None:
        super().__init__(identifier, metadata, metadata_location, io, catalog)
        self.partitions = None

    @classmethod
    def from_pyiceberg_table(cls, table: Table):
        return cls(table.identifier, table.metadata, table.metadata_location, table.io, table.catalog)

    def precomp_partitions(self):
        if self.spec().is_unpartitioned():
            self.partitions = []

        partition_cols_ids = [p["source-id"] for p in self.spec().model_dump()["fields"]]
        self.partitions = [c["name"] for c in self.schema().model_dump()["fields"] if c["id"] in partition_cols_ids]

    def __repr__(self) -> str:
        return self.table


class TableWithAlias:
    """
    Dataclass contains table name with alias
    """

    def __init__(self, tname: str, talias: str) -> None:
        self.table_name: str = tname
        self.table_alias: str = talias
        self.comparisons: BooleanExpression = None

    @classmethod
    def from_identifier(cls, identf: sqlparse.sql.Identifier):
        return cls(identf.get_real_name(), identf.get_alias())

    def set_comparisons(self, comparisons: BooleanExpression):
        self.comparisons = comparisons
        return self

    def __str__(self) -> str:
        return f"{self.table_name} ({self.table_alias})"

    def __repr__(self) -> str:
        return self.__str__()
