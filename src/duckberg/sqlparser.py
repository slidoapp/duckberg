import sqlparse
from pyiceberg.expressions import *
from pyiceberg.expressions import parser

from duckberg.table import TableWithAlias


class DuckBergSQLParser:
    def parse_first_query(self, sql: str) -> sqlparse.sql.Statement:
        reformated_sql = sql.replace("'", '"')  # replace all single quotes with double quotes
        return sqlparse.parse(reformated_sql)[0]

    def unpack_identifiers(self, token: sqlparse.sql.IdentifierList) -> list[TableWithAlias]:
        return list(
            map(
                lambda y: TableWithAlias.from_identifier(y),
                filter(lambda x: type(x) is sqlparse.sql.Identifier, token.tokens),
            )
        )

    def extract_tables(self, parsed_sql: sqlparse.sql.Statement) -> list[TableWithAlias]:
        tables = []
        get_next = 0
        c_table: list[TableWithAlias] = []
        c_table_wc = None
        for token in parsed_sql.tokens:
            if get_next == 1 and token.ttype is not sqlparse.tokens.Whitespace:
                if type(token) is sqlparse.sql.Identifier:
                    c_table = [TableWithAlias.from_identifier(token)]
                    get_next += 1
                elif type(token) is sqlparse.sql.IdentifierList:
                    c_table = self.unpack_identifiers(token)
                    get_next += 1
                elif type(token) is sqlparse.sql.Parenthesis:
                    tables.extend(self.extract_tables(token))

            if token.ttype is sqlparse.tokens.Keyword and str(token.value).upper() == "FROM":
                get_next += 1

            if type(token) is sqlparse.sql.Where:
                c_table_wc = self.extract_where_conditions(token)

        mapped_c_table = list(map(lambda x: x.set_comparisons(c_table_wc), c_table))
        tables.extend(mapped_c_table)
        return tables

    def extract_where_conditions(self, where_statement: list[sqlparse.sql.Where]):
        comparison = sqlparse.sql.TokenList(where_statement[1:])
        return parser.parse(str(comparison))
