import pytest

from duckberg.sqlparser import DuckBergSQLParser


@pytest.fixture
def get_parser():
    return DuckBergSQLParser()


def test_select_where_1(get_parser):
    sql = """SELECT * FROM this_is_awesome_table WHERE a > 15"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    res_where = str(res[0].comparisons)
    assert "GreaterThan(term=Reference(name='a'), literal=LongLiteral(15))" == res_where


def test_select_where_2(get_parser):
    sql = """SELECT * FROM this_is_awesome_table WHERE a > 15 AND a < 16"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    res_where = str(res[0].comparisons)
    assert (
        "And(left=GreaterThan(term=Reference(name='a'), literal=LongLiteral(15)), right=LessThan(term=Reference(name='a'), literal=LongLiteral(16)))"
        == res_where
    )


def test_select_where_3(get_parser):
    sql = """SELECT * FROM this_is_awesome_table WHERE (a > 15 AND a < 16) OR c > 15"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    res_where = str(res[0].comparisons)
    assert (
        "Or(left=And(left=GreaterThan(term=Reference(name='a'), literal=LongLiteral(15)), right=LessThan(term=Reference(name='a'), literal=LongLiteral(16))), right=GreaterThan(term=Reference(name='c'), literal=LongLiteral(15)))"
        == res_where
    )


def test_select_where_4(get_parser):
    sql = """SELECT * FROM this_is_awesome_table WHERE (b = "test string" AND a < 16) OR c > 15"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    res_where = str(res[0].comparisons)
    assert (
        "Or(left=And(left=EqualTo(term=Reference(name='b'), literal=literal('test string')), right=LessThan(term=Reference(name='a'), literal=LongLiteral(16))), right=GreaterThan(term=Reference(name='c'), literal=LongLiteral(15)))"
        == res_where
    )


def test_select_where_4(get_parser):
    sql = """SELECT * FROM this_is_awesome_table WHERE (b = "test string" AND column = '108e6307-f23a-4e10-9e38-1866d58b4355') OR c > 15"""
    sql_parsed = get_parser.parse_first_query(sql=sql)
    res = get_parser.extract_tables(sql_parsed)
    res_where = str(res[0].comparisons)
    assert (
        "Or(left=And(left=EqualTo(term=Reference(name='b'), literal=literal('test string')), right=EqualTo(term=Reference(name='column'), literal=literal('108e6307-f23a-4e10-9e38-1866d58b4355'))), right=GreaterThan(term=Reference(name='c'), literal=LongLiteral(15)))"
        == res_where
    )
