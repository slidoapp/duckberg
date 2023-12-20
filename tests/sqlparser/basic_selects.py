from duckberg.sqlparser import DuckBergSQLParser


parser = DuckBergSQLParser()


sql1 = """
SELECT * FROM this_is_awesome_table"""
sql1_parsed = parser.parse_first_query(sql=sql1)
res1 = parser.extract_tables(sql1_parsed)
assert len(res1) == 1
assert list(map(lambda x: str(x), res1)) == ["this_is_awesome_table (None)"]

sql2 = """
SELECT * FROM this_is_awesome_table, second_awesome_table"""
sql2_parsed = parser.parse_first_query(sql=sql2)
res2 = parser.extract_tables(sql2_parsed)
assert len(res2) == 2
assert list(map(lambda x: str(x), res2)) == ["this_is_awesome_table (None)", "second_awesome_table (None)"]

sql3 = """
SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table))"""
sql3_parsed = parser.parse_first_query(sql=sql3)
res3 = parser.extract_tables(sql3_parsed)
assert len(res3) == 1
assert list(map(lambda x: str(x), res3)) == ["this_is_awesome_table (None)"]

sql4 = """
SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table), second_awesome_table)"""
sql4_parsed = parser.parse_first_query(sql=sql4)
res4 = parser.extract_tables(sql4_parsed)
assert len(res4) == 2
assert list(map(lambda x: str(x), res4)) == ["this_is_awesome_table (None)", "second_awesome_table (None)"]

sql5 = """
SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table tiat, second_awesome_table))"""
sql5_parsed = parser.parse_first_query(sql=sql5)
res5 = parser.extract_tables(sql5_parsed)
assert len(res5) == 2
assert list(map(lambda x: str(x), res5)) == ["this_is_awesome_table (tiat)", "second_awesome_table (None)"]
