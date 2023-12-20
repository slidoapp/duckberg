import pytest

from duckberg.sqlparser import DuckBergSQLParser


@pytest.fixture
def get_parser():
    return DuckBergSQLParser()


def test_basic_select_1(get_parser):
    sql1 = """
  SELECT * FROM this_is_awesome_table"""
    sql1_parsed = get_parser.parse_first_query(sql=sql1)
    res1 = get_parser.extract_tables(sql1_parsed)
    assert len(res1) == 1
    assert list(map(lambda x: str(x), res1)) == ["this_is_awesome_table (None)"]


def test_basic_select_2(get_parser):
    sql2 = """
  SELECT * FROM this_is_awesome_table, second_awesome_table"""
    sql2_parsed = get_parser.parse_first_query(sql=sql2)
    res2 = get_parser.extract_tables(sql2_parsed)
    assert len(res2) == 2
    assert list(map(lambda x: str(x), res2)) == ["this_is_awesome_table (None)", "second_awesome_table (None)"]


def test_basic_select_3(get_parser):
    sql3 = """
  SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table))"""
    sql3_parsed = get_parser.parse_first_query(sql=sql3)
    res3 = get_parser.extract_tables(sql3_parsed)
    assert len(res3) == 1
    assert list(map(lambda x: str(x), res3)) == ["this_is_awesome_table (None)"]


def test_basic_select_4(get_parser):
    sql4 = """
  SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table), second_awesome_table)"""
    sql4_parsed = get_parser.parse_first_query(sql=sql4)
    res4 = get_parser.extract_tables(sql4_parsed)
    assert len(res4) == 2
    assert list(map(lambda x: str(x), res4)) == ["this_is_awesome_table (None)", "second_awesome_table (None)"]


def test_basic_select_5(get_parser):
    sql5 = """
  SELECT * FROM (SELECT * FROM (SELECT * FROM this_is_awesome_table tiat, second_awesome_table))"""
    sql5_parsed = get_parser.parse_first_query(sql=sql5)
    res5 = get_parser.extract_tables(sql5_parsed)
    assert len(res5) == 2
    assert list(map(lambda x: str(x), res5)) == ["this_is_awesome_table (tiat)", "second_awesome_table (None)"]
