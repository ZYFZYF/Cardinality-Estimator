import sqlparse
import pdb
import pandas as pd
import time
from collections import defaultdict
from scipy.stats import gaussian_kde
from table import Table
import matplotlib.pyplot as plt

tables = defaultdict(Table)
is_int = {}
data_index = {}
int_index = {}
int_columns = defaultdict(list)

start_time = time.time()


def record_time(message):
    global start_time
    end_time = time.time()
    print(f"{message} : {round(end_time - start_time, 3)}s")
    start_time = time.time()


def init():
    global tables
    for create_sql in sqlparse.parse(open(f'data/schematext.sql').read())[:-1]:
        column_list = []
        for token in create_sql.tokens:
            if type(token) == sqlparse.sql.Identifier:
                table_name = token.value
            if type(token) == sqlparse.sql.Parenthesis:
                for tk in token.tokens:
                    if type(tk) == sqlparse.sql.Identifier:
                        column_name = tk.value
                        # print(column_name)
                    if type(tk) == sqlparse.sql.IdentifierList:
                        for id in tk.get_identifiers():
                            if type(id) == sqlparse.sql.Identifier or id.value in ['link', 'role']:
                                column_name = id.value
                    # print('TAT', tk.value)
                    if tk.value == 'integer':
                        column_list.append((column_name, int))
                    if tk.value == 'character':
                        column_list.append((column_name, str))
        tables[table_name] = Table(table_name, column_list)
        # record_time(f'init table {table_name}')


def solve(sql):
    sql = sqlparse.parse(sql)[0]
    related_tables = {}
    selections = {}
    joins = []
    for token in sql.tokens:
        if type(token) == sqlparse.sql.IdentifierList:
            # 多张表名
            for tk in token.tokens:
                if type(tk) == sqlparse.sql.Identifier:
                    related_tables[tk.get_alias()] = tk.get_real_name()
            for table in related_tables.keys():
                selections[table] = defaultdict(list)
        if type(token) == sqlparse.sql.Identifier:
            # 单张表名
            related_tables[token.get_alias()] = token.get_real_name()
            for table in related_tables.keys():
                selections[table] = defaultdict(list)
            # if isinstance(token, sqlparse.sql.Identifier):
            #     token.ge
        if type(token) == sqlparse.sql.Where:
            # where 语句
            for ind, tk in enumerate(token.tokens):
                if type(tk) == sqlparse.sql.Comparison:
                    tokens = list(filter(lambda x: x.value != ' ', tk.tokens))
                    # print([tk.value for tk in tokens])
                    if type(tokens[2]) == sqlparse.sql.Token:
                        # 如果不是Join条件
                        table = tokens[0].get_parent_name()
                        column = tokens[0].get_name()
                        cmp = tokens[1].value
                        value = eval(tokens[2].value)
                        # 是like的话
                        if cmp in ['LIKE', 'NOT LIKE']:
                            selections[table][column].append((cmp, value))
                        # 是其他比较的话
                        else:
                            # selection
                            # print('select ', tk.value)
                            if column in selections[table].keys():
                                for c, v in zip([cmp, selections[table][column][0][0]],
                                                [value, selections[table][column][0][1]]):
                                    # 合并区间
                                    if c == '<':
                                        upper = v - 1
                                    if c == '<=':
                                        upper = v
                                    if c == '>':
                                        lower = v + 1
                                    if c == '>=':
                                        lower = v
                                selections[table][column] = [('BETWEEN', (lower, upper))]
                            else:
                                selections[table][column].append((cmp, value))
                            if cmp not in ['<', '>', '<=', '>=', '!=', '=']:
                                print(cmp, tk.value)
                        # print(cmp, selections[table][column])
                    else:
                        # join
                        # print('join ', tk.value)
                        joins.append(((tokens[0].get_parent_name(), tokens[0].get_name()),
                                      (tokens[2].get_parent_name(), tokens[2].get_name())))
                    # print(tk.value, [type(t) for t in tk.tokens], ' '.join([t.value for t in tk.tokens]))
                else:
                    if tk.value.find('IN') != -1:
                        # print(tk.value)
                        # print(token.tokens[ind + 1], type(token.tokens[ind + 1]), token.tokens[ind + 2],
                        #       type(token.tokens[ind + 2]))
                        pass
                    if type(tk) == sqlparse.sql.Identifier:
                        table = tk.get_parent_name()
                        column = tk.get_name()
                        if token.tokens[ind + 2].value == 'BETWEEN':
                            selections[table][column].append(('BETWEEN', (
                                eval(token.tokens[ind + 4].value), eval(token.tokens[ind + 8].value))))
                            # print(table, column, selections[table][column])
                        elif token.tokens[ind + 2].value == 'IN':
                            value = eval(token.tokens[ind + 4].value)
                            if type(value) != tuple:
                                value = [value]
                            else:
                                value = list(value)
                            selections[table][column].append(('IN', value))
                        # print(table, column, token.tokens[ind + 1], token.tokens[ind + 2], token.tokens[ind + 3])

                    # print(tk, type(tk), tk.ttype, tk.value)
    # print('related_tables : ', related_tables)
    # print('selection : ', selections)
    # print('joins : ', joins)
    # 从joins关系找出等价类
    father = {}

    def get_father(x):
        if father[x] == x:
            return x
        else:
            y = get_father(father[x])
            father[x] = y
            return y

    def union(x, y):
        father[get_father(x)] = get_father(y)

    join_attrs = []
    for join in joins:
        father[join[0]] = join[0]
        father[join[1]] = join[1]
        join_attrs.extend([join[0], join[1]])
    for join in joins:
        union(join[0], join[1])

    equal_class = defaultdict(list)
    join_attrs = set(join_attrs)
    for attr in join_attrs:
        equal_class[get_father(attr)].append(attr)
    for ind, v in enumerate(equal_class.values()):
        print(ind, v)

    # 计算U数组，每个表被在join attr里的个数
    U = defaultdict(int)
    for attr in join_attrs:
        U[attr[0]] += 1
    for k, v in U.items():
        print(k, v)
    return 1


if __name__ == '__main__':
    init()
    record_time('Init all things')
    data = {}
    for sql_file in ['hard']:  # , 'middle','easy']:
        raw = open(f'input/{sql_file}.sql').read()
        sql_stats = sqlparse.split(raw)
        ground_true = list(map(int, open(f'answer/{sql_file}.normal').readlines()))
        scores = []
        results = []
        for sql, truth in zip(sql_stats, ground_true):
            predict = solve(sql)
            score = round(max(predict, truth) / (min(predict, truth) + 0.1), 3)
            scores.append(score)
            results.append(predict)
        record_time(f'Estimate {sql_file}')
        df = pd.DataFrame(data={sql_file: scores, 'predict': results, 'truth': ground_true})
        df.to_csv(f'output/{sql_file}.csv', index=False)
        print(df.describe())
