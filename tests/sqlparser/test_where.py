import pytest

from duckberg.sqlparser import DuckBergSQLParser


@pytest.fixture
def get_parser():
    return DuckBergSQLParser()


def test_select_where_1(get_parser):
    sql1 = """
    SELECT * FROM this_is_awesome_table WHERE a > 15"""
    sql1_parsed = get_parser.parse_first_query(sql=sql1)
    res1 = get_parser.extract_tables(sql1_parsed)
    res1_where = str(res1[0].comparisons)
    assert "GreaterThan(term=Reference(name='a'), literal=LongLiteral(15))" == res1_where


def test_select_where_2(get_parser):
    sql2 = """
    SELECT * FROM this_is_awesome_table WHERE a > 15 AND a < 16"""
    sql2_parsed = get_parser.parse_first_query(sql=sql2)
    res2 = get_parser.extract_tables(sql2_parsed)
    res2_where = str(res2[0].comparisons)
    assert (
        "And(left=GreaterThan(term=Reference(name='a'), literal=LongLiteral(15)), right=LessThan(term=Reference(name='a'), literal=LongLiteral(16)))"
        == res2_where
    )


def test_select_where_3(get_parser):
    sql3 = """
    SELECT * FROM this_is_awesome_table WHERE (a > 15 AND a < 16) OR c > 15"""
    sql3_parsed = get_parser.parse_first_query(sql=sql3)
    res3 = get_parser.extract_tables(sql3_parsed)
    res3_where = str(res3[0].comparisons)
    assert (
        "Or(left=And(left=GreaterThan(term=Reference(name='a'), literal=LongLiteral(15)), right=LessThan(term=Reference(name='a'), literal=LongLiteral(16))), right=GreaterThan(term=Reference(name='c'), literal=LongLiteral(15)))"
        == res3_where
    )
