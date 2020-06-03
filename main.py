import sqlparse
import pdb
import pandas as pd
import time
from collections import defaultdict
from scipy.stats import gaussian_kde
from table import *
import matplotlib.pyplot as plt
import random

tables = defaultdict(Table)
is_int = {}
data_index = {}
int_index = {}
int_columns = defaultdict(list)

start_time = time.time()

J = 0


def record_time(message):
    global start_time
    end_time = time.time()
    print(f"{message} : {round(end_time - start_time, 3)}s")
    start_time = time.time()


father = {}


def init():
    # 先把所有join的信息读出来，把column聚类
    all_joins = []
    for file in ['easy', 'middle', 'hard']:
        for query in sqlparse.split(open(f'input/{file}.sql').read())[:-1]:
            alias_to_table, _, joins = parse(query)
            joins = [((alias_to_table[join[0][0]], join[0][1]), (alias_to_table[join[1][0]], join[1][1])) for join in
                     joins]
            all_joins.extend(joins)

    attrs = list(set([join[0] for join in all_joins] + [join[1] for join in all_joins]))
    # 从joins关系找出等价类
    global father
    father = {attr: attr for attr in attrs}

    def get_father(x):
        if father[x] == x:
            return x
        else:
            y = get_father(father[x])
            father[x] = y
            return y

    def union(x, y):
        father[get_father(x)] = get_father(y)

    for join in all_joins:
        union(join[0], join[1])

    for attr in sorted(attrs):
        father[attr] = get_father(attr)
        # print(attr, father[attr])

    aggregate_clusters = defaultdict(list)
    for attr in attrs:
        aggregate_clusters[get_father(attr)].append(attr)

    for k, v in aggregate_clusters.items():
        print(k, v)
    join_attrs = defaultdict(list)
    for attr in attrs:
        join_attrs[attr[0]].append(attr[1])

    for k, v in join_attrs.items():
        print(k, v)

    # 为每个等价类制定一个hash函数
    H = {}
    for k in aggregate_clusters.keys():
        MOD = 1000000007
        H[k] = (random.randint(1, MOD), random.randint(1, MOD), MOD)

    record_time('Aggregate join columns')

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
        tables[table_name] = Table(table_name, column_list, join_attrs, H, father)
        record_time(f'Init table {table_name}')
    record_time('Finish sample tables')


def parse(query_sql):
    sql = sqlparse.parse(query_sql)[0]
    alias_to_table = {}
    selections = {}
    joins = []
    for token in sql.tokens:
        if type(token) == sqlparse.sql.IdentifierList:
            # 多张表名
            for tk in token.tokens:
                if type(tk) == sqlparse.sql.Identifier:
                    alias_to_table[tk.get_alias()] = tk.get_real_name()
            for alias in alias_to_table.keys():
                selections[alias] = defaultdict(list)
        if type(token) == sqlparse.sql.Identifier:
            # 单张表名
            alias_to_table[token.get_alias()] = token.get_real_name()
            for alias in alias_to_table.keys():
                selections[alias] = defaultdict(list)
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
                        alias = tokens[0].get_parent_name()
                        column = tokens[0].get_name()
                        cmp = tokens[1].value
                        value = eval(tokens[2].value)
                        # 是like的话
                        if cmp in ['LIKE', 'NOT LIKE']:
                            selections[alias][column].append((cmp, value))
                        # 是其他比较的话
                        else:
                            # selection
                            # print('select ', tk.value)
                            if column in selections[alias].keys():
                                for c, v in zip([cmp, selections[alias][column][0][0]],
                                                [value, selections[alias][column][0][1]]):
                                    # 合并区间
                                    if c == '<':
                                        upper = v - 1
                                    if c == '<=':
                                        upper = v
                                    if c == '>':
                                        lower = v + 1
                                    if c == '>=':
                                        lower = v
                                selections[alias][column] = [('BETWEEN', (lower, upper))]
                            else:
                                selections[alias][column].append((cmp, value))
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
                        alias = tk.get_parent_name()
                        column = tk.get_name()
                        if token.tokens[ind + 2].value == 'BETWEEN':
                            selections[alias][column].append(('BETWEEN', (
                                eval(token.tokens[ind + 4].value), eval(token.tokens[ind + 8].value))))
                            # print(table, column, selections[table][column])
                        elif token.tokens[ind + 2].value == 'IN':
                            value = eval(token.tokens[ind + 4].value)
                            if type(value) != tuple:
                                value = [value]
                            else:
                                value = list(value)
                            selections[alias][column].append(('IN', value))
                        # print(table, column, token.tokens[ind + 1], token.tokens[ind + 2], token.tokens[ind + 3])

                    # print(tk, type(tk), tk.ttype, tk.value)
    # print('related_tables : ', related_tables)
    # print('selection : ', selections)
    # print('joins : ', joins)
    return alias_to_table, selections, joins


index = 0


def solve(sql):
    global index
    index += 1
    alias_to_table, selections, joins = parse(sql)
    join_tables = list(alias_to_table.keys())
    join_items = list(set([join[0] for join in joins] + [join[1] for join in joins]))
    # 得到采样之后join的结果大小 J
    global J
    J = 0
    # 如果是单表select的话，直接采样即可
    if len(joins) == 0:
        alias_sample_size = {alias: SAMPLE_SIZE[0] for alias in join_tables}
        alias = list(alias_to_table.keys())[0]
        table = alias_to_table[alias]
        sample_size = SAMPLE_SIZE[0]
        for value in tables[table].sample_values[sample_size]:
            if tables[table].satisfy(value, selections[alias]):
                J += 1
    # 如果是双表join的话，直接用桶即可
    elif len(joins) == 1:
        alias_sample_size = {alias: SAMPLE_SIZE[0] for alias in join_tables}
        sample_size = SAMPLE_SIZE[0]
        Count = {alias: defaultdict(int) for alias in join_tables}
        for item in join_items:
            alias, attr = item
            table = alias_to_table[alias]
            for value in tables[table].sample_values[sample_size]:
                if tables[table].satisfy(value, selections[alias]):
                    Count[alias][value[tables[table].column_ind[attr]]] += 1
        for v in list(Count[join_items[0][0]].keys()):
            J += Count[join_items[0][0]][v] * Count[join_items[1][0]][v]
    else:
        compare_columns = defaultdict(list)
        for join in joins:
            compare_columns[join[0]].append(join[1])
            compare_columns[join[1]].append(join[0])

        row = {}
        join_tables = sorted(join_tables, key=lambda x: len(selections[x]), reverse=True)
        S = {alias: defaultdict(list) for alias in join_tables}
        alias_sample_size = {alias: SAMPLE_SIZE[-1] for alias in join_tables}

        for sample_size in reversed(SAMPLE_SIZE):
            for alias in join_tables:
                table = alias_to_table[alias]
                for value in tables[table].sample_values[sample_size]:
                    if tables[table].satisfy(value, selections[alias]):
                        S[alias][sample_size].append(value)
                if len(S[alias][sample_size]) < 10000 ** (1.0 / len(join_tables)):
                    alias_sample_size[alias] = sample_size

        estimate_cost = 1
        for alias in join_tables:
            estimate_cost *= len(S[alias][alias_sample_size[alias]])
        print(
            f'Estimate row list is {[len(tables[alias_to_table[alias]].sample_values[alias_sample_size[alias]]) for alias in join_tables]} and select result is {[len(S[alias][alias_sample_size[alias]]) for alias in join_tables]} and total cost is {estimate_cost}')

        def dfs(ind):
            if ind == len(join_tables):
                global J
                J += 1
                return
            alias = join_tables[ind]
            table = alias_to_table[alias]
            for value in S[alias][alias_sample_size[alias]]:
                flag = True
                for attr in tables[table].u:
                    for compare_column in compare_columns[(alias, attr)]:
                        if compare_column in row.keys():
                            if value[tables[table].column_ind[attr]] != row[compare_column]:
                                flag = False
                                break
                    if not flag:
                        break
                if flag:
                    for attr in tables[table].u:
                        row[(alias, attr)] = value[tables[table].column_ind[attr]]
                    dfs(ind + 1)
            for attr in tables[table].u:
                if (alias, attr) in row.keys():
                    row.pop((alias, attr))

        dfs(0)
    print(f'{index}: Sample join answer is {J}, having {len(joins)} joins')
    # 计算得到Pinc

    Pinc = 1
    if len(joins) > 0:
        equal_class = list(set([father[(alias_to_table[item[0]], item[1])] for item in join_items]))
        for eq_cls in equal_class:
            mi = 1
            for attr in join_items:
                table = alias_to_table[attr[0]]
                if father[(table, attr[1])] == eq_cls:
                    print(attr[0], attr[1], table, tables[table].U)
                    mi = min(mi, tables[table].P[alias_sample_size[attr[0]]] ** (1.0 / tables[table].U))
            Pinc *= mi
    else:
        alias = list(alias_to_table.keys())[0]
        table = alias_to_table[alias]
        Pinc = tables[table].P[SAMPLE_SIZE[0]]
    if J == 0:
        J = random.random()
    return round(J / Pinc)


def evaluate():
    for sql_file in ['middle', 'hard']:
        df = pd.read_csv(f'output/{sql_file}.csv')
        print(df.describe())


if __name__ == '__main__':
    init()
    data = {}
    for sql_file in ['hard', 'middle', 'hard']:
        index = 0
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
