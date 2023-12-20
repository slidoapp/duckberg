from duckberg.sqlparser import DuckBergSQLParser


parser = DuckBergSQLParser()


sql1 = """
SELECT * FROM this_is_awesome_table WHERE a > 15"""
sql1_parsed = parser.parse_first_query(sql=sql1)
res1 = parser.extract_tables(sql1_parsed)
res1_where = str(res1[0].comparisons)
assert "GreaterThan(term=Reference(name='a'), literal=LongLiteral(15))" == res1_where

sql2 = """
SELECT * FROM this_is_awesome_table WHERE a > 15 AND a < 16"""
sql2_parsed = parser.parse_first_query(sql=sql2)
res2 = parser.extract_tables(sql2_parsed)
res2_where = str(res2[0].comparisons)
assert (
    "And(left=GreaterThan(term=Reference(name='a'), literal=LongLiteral(15)), right=LessThan(term=Reference(name='a'), literal=LongLiteral(16)))"
    == res2_where
)

sql3 = """
SELECT * FROM this_is_awesome_table WHERE (a > 15 AND a < 16) OR c > 15"""
sql3_parsed = parser.parse_first_query(sql=sql3)
res3 = parser.extract_tables(sql3_parsed)
res3_where = str(res3[0].comparisons)
assert (
    "Or(left=And(left=GreaterThan(term=Reference(name='a'), literal=LongLiteral(15)), right=LessThan(term=Reference(name='a'), literal=LongLiteral(16))), right=GreaterThan(term=Reference(name='c'), literal=LongLiteral(15)))"
    == res3_where
)
