import sys
import pytest
import sqlparse
import re
from sqlparse.sql import IdentifierList, Identifier, Token, Where, Parenthesis
from sqlparse.tokens import Keyword, DML

class SQLParser:
  @staticmethod
  def extract_table_from_subquery(subquery):
    from_seen = False
    for token in subquery.tokens:
      if from_seen:
        if isinstance(token, Identifier):
          return token.get_real_name()
        elif isinstance(token, IdentifierList):
          return token.get_identifiers()[0].get_real_name()
      elif token.ttype is Keyword and token.value.upper() == 'FROM':
        from_seen = True
    return None

  @staticmethod
  def process_identifier(identifier):
    if identifier.has_alias():
      subquery = identifier.tokens[0]
      alias = identifier.get_alias()
      if isinstance(subquery, Parenthesis):
        subquery_table = SQLParser.extract_table_from_subquery(subquery)
        return f"sub:{subquery_table or alias}"
    return identifier.get_real_name()

  @staticmethod
  def extract_tables(parsed, keyword='FROM', is_join=False):
    keyword_seen = False
    for item in parsed.tokens:
      if keyword_seen:
        if isinstance(item, Identifier):
          yield SQLParser.process_identifier(item)
        elif isinstance(item, IdentifierList):
          for identifier in item.get_identifiers():
            yield SQLParser.process_identifier(identifier)
        elif item.ttype is Keyword:
          return
      elif item.ttype is Keyword and is_join and SQLParser.is_join_clause(item.value.upper()):
        keyword_seen = True
      elif item.ttype is Keyword and item.value.upper() == keyword:
        keyword_seen = True

  @staticmethod
  def is_join_clause(sql_str):
    # 正規表現パターン
    # https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py#L70-L71
    join_pattern = r'((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN\b'
    # re.search() を使用してパターンにマッチするか確認
    match = re.search(join_pattern, sql_str, re.IGNORECASE)
    # マッチした場合はTrue、そうでない場合はFalseを返す
    return bool(match)

  @staticmethod
  def abstract_conditions(conditions):
    conditions = re.sub(r'\b\d+(?:\.\d+)?\b', '9', conditions)
    conditions = re.sub(r"'[^']*'", "'X'", conditions)
    conditions = re.sub(r'"[^"]*"', '"X"', conditions)
    conditions = re.sub(r'\bTRUE\b|\bFALSE\b', 'B', conditions, flags=re.IGNORECASE)
    conditions = re.sub(r'\bNULL\b', 'NULL', conditions, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', conditions).strip()

  @staticmethod
  def extract_conditions(parsed, clause_type=Where):
    clause_item = next((item for item in parsed.tokens if isinstance(item, clause_type)), None)
    if clause_item:
      conditions = ' '.join(str(token) for token in clause_item.tokens if not token.is_whitespace and token.value.upper() != clause_type.__name__.upper()).strip()
      return SQLParser.abstract_conditions(conditions)
    return ''

  @staticmethod
  def extract_subquery_info(subquery):
    # サブクエリのカッコを外す
    subquery = str(subquery).strip('()')

    parsed_subquery = sqlparse.parse(subquery)[0]
    tables = list(SQLParser.extract_tables(parsed_subquery))
    conditions = SQLParser.extract_conditions(parsed_subquery)
    return tables, conditions

  @staticmethod
  def process_subqueries(conditions):
    def replace_subquery(match):
      subquery = match.group(0)
      tables, sub_conditions = SQLParser.extract_subquery_info(subquery)
      return f"(sub:{','.join(tables)}|{sub_conditions})"

    return re.sub(r'\(SELECT[^)]+\)', replace_subquery, conditions, flags=re.IGNORECASE)

  @staticmethod
  def parse_sql(sql):
    parsed = sqlparse.parse(sql)[0]
    from_tables = list(SQLParser.extract_tables(parsed, 'FROM'))
    join_tables = list(SQLParser.extract_tables(parsed, 'JOIN', True))
    where_conditions = SQLParser.extract_conditions(parsed)
    where_conditions = SQLParser.process_subqueries(where_conditions)
    
    return {
      'from_tables': from_tables,
      'join_tables': join_tables,
      'where_conditions': where_conditions
    }

  # NOTE: このClassでカテゴライズしたい時はこちらを活かす
  # @staticmethod
  # def categorize_sql(sql_list):
  #   categories = {}
  #   for sql in sql_list:
  #     parsed_info = SQLParser.parse_sql(sql)
  #     key = (
  #       tuple(sorted(parsed_info['from_tables'])),
  #       tuple(sorted(parsed_info['join_tables'])),
  #       parsed_info['where_conditions']
  #     )
  #     if key not in categories:
  #       categories[key] = []
  #     categories[key].append(sql)
  #   return categories

  @staticmethod
  def categorize_sql(sql):
    parsed_info = SQLParser.parse_sql(sql)
    return (
      tuple(sorted(parsed_info['from_tables'])),
      tuple(sorted(parsed_info['join_tables'])),
      parsed_info['where_conditions']
    )

# ------------------------------------------------------------------------------
# TEST
# ------------------------------------------------------------------------------
class TestClass:
  @pytest.fixture
  def sql_list(self):
    return [
      "SELECT * FROM table1 WHERE id = 1",
      "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.name = 'John' AND table2.age > 30",
      "SELECT * FROM table2 WHERE age > 30 AND status = 'active'",
      "SELECT * FROM table3 WHERE price BETWEEN 100 AND 200",
      "SELECT * FROM table4 WHERE date BETWEEN '2023-01-01' AND '2023-12-31' AND category IN ('A', 'B', 'C')",
      "SELECT * FROM table5 WHERE (status = 'pending' OR status = 'processing') AND priority > 5",
      "SELECT * FROM table6 WHERE id IN (SELECT id FROM table7 WHERE value > 100)",
      "SELECT * FROM (SELECT id, name FROM table8 WHERE status = 'active') subquery WHERE subquery.id > 10",
      "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE country = 'USA' AND age > 18) AND ACTIVE = true",
      "SELECT * FROM table1 LEFT  JOIN table2 ON table1.id = table2.id WHERE table1.name = 'John' AND table2.age > 30",
    ]

  @pytest.fixture
  def expected_parser(self):
    return [
      (('table1',), (), 'id = 9'),
      (('table1',), ('table2',), "table1.name = 'X' AND table2.age > 9"),
      (('table2',), (), "age > 9 AND status = 'X'"),
      (('table3',), (), 'price BETWEEN 9 AND 9'),
      (('table4',), (), "date BETWEEN 'X' AND 'X' AND category IN ('X', 'X', 'X')"),
      (('table5',), (), "(status = 'X' OR status = 'X') AND priority > 9"),
      (('table6',), (), 'id IN (sub:table7|value > 9)'),
      (('sub:table8',), (), 'subquery.id > 9'),
      (('orders',), (), "customer_id IN (sub:customers|country = 'X' AND age > 9) AND ACTIVE = B"),
      (('table1',), ('table2',), "table1.name = 'X' AND table2.age > 9"),
    ]

  def test_checkSQL(self, sql_list, expected_parser):
    for sql, expected_result in zip(sql_list, expected_parser):
      parsed_result = SQLParser.categorize_sql(sql)
      assert parsed_result == expected_result, f"Failed for SQL: {sql}"

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
  for line in sys.stdin:
    line = line.strip()
    print(SQLParser.categorize_sql(line))

if __name__ == "__main__":
  main()
