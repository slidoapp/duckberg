import pytest

from duckberg.sqlparser import DuckBergSQLParser


@pytest.fixture
def get_parser() -> DuckBergSQLParser:
    return DuckBergSQLParser()


def test_basic_select_1(get_parser):
    sql = """SELECT * FROM this_is_awesome_table"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    assert len(res) == 1
    assert list(map(lambda x: str(x), res)) == ["this_is_awesome_table (None)"]


def test_basic_select_2(get_parser):
    sql = """SELECT * FROM 'this_is_awesome_table'"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    print(str(sql_parsed.tokens))
    res = get_parser.extract_tables(sql_parsed)
    print(res)
    assert len(res) == 1
    assert list(map(lambda x: str(x), res)) == ["this_is_awesome_table (None)"]


def test_basic_select_3(get_parser):
    sql = """SELECT * FROM this_is_awesome_table, second_awesome_table"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    assert len(res) == 2
    assert list(map(lambda x: str(x), res)) == ["this_is_awesome_table (None)", "second_awesome_table (None)"]


def test_basic_select_4(get_parser):
    sql = """SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table))"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    assert len(res) == 1
    assert list(map(lambda x: str(x), res)) == ["this_is_awesome_table (None)"]


def test_basic_select_5(get_parser):
    sql = """SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table), second_awesome_table)"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    assert len(res) == 2
    assert list(map(lambda x: str(x), res)) == ["this_is_awesome_table (None)", "second_awesome_table (None)"]


def test_basic_select_6(get_parser):
    sql = """SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table tiat, second_awesome_table))"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    assert len(res) == 2
    assert list(map(lambda x: str(x), res)) == ["this_is_awesome_table (tiat)", "second_awesome_table (None)"]
