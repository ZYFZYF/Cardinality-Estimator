import sqlparse
from sqlparse.sql import *

tables = {}


def init():
    global tables
    for create_sql in sqlparse.parse(open(f'data/schematext.sql').read())[:-1]:
        print(create_sql)
        column_list = []
        for token in create_sql.tokens:
            if isinstance(token, sqlparse.sql.Identifier):
                table_name = token.value
            if isinstance(token, sqlparse.sql.Parenthesis):
                for tk in token.tokens:
                    if isinstance(tk, sqlparse.sql.Identifier):
                        column_name = tk.value
                        # print(column_name)
                    if isinstance(tk, sqlparse.sql.IdentifierList):
                        for id in tk.get_identifiers():
                            if isinstance(id, sqlparse.sql.Identifier):
                                column_name = id.value
                    # print('TAT', tk.value)
                    if tk.value == 'integer':
                        column_list.append((column_name, int))
                    if tk.value == 'character':
                        column_list.append((column_name, str))
        tables[table_name] = column_list
    # print(tables)


def solve(sql):
    return 1


if __name__ == '__main__':
    init()
    for sql_file in ['easy', 'middle', 'hard']:
        raw = open(f'input/{sql_file}.sql').read()
        ground_true = open(f'answer/{sql_file}.normal').readlines()
        for sql, truth in zip(sqlparse.split(raw), ground_true):
            predict = solve(sql)
            # print(sql, truth)
