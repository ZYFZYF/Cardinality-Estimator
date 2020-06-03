import sqlparse
import pdb
import pandas as pd
import time
from collections import defaultdict
from scipy.stats import gaussian_kde
from table import Table
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
    join_attrs = list(set(join_attrs))
    for attr in join_attrs:
        equal_class[get_father(attr)].append(attr)
    for ind, v in enumerate(equal_class.values()):
        print(ind, v)

    # 计算U数组，每个表被在join attr里的个数 还有每个表被join的参数
    U = defaultdict(int)
    u = defaultdict(list)
    for attr in join_attrs:
        U[attr[0]] += 1
        u[attr[0]].append(attr[1])
    # for k, v in U.items():
    #     print(k, v)

    join_tables = list(alias_to_table.keys())
    # 确定每个表的采样概率P
    P = {}
    for alias in join_tables:
        P[alias] = min(1.0, 1.0 * 1000 / tables[alias_to_table[alias]].row_number)

    # 先确定一个表采样10000条
    # first_table = join_tables[0]
    # P[first_table] = 1.0 * 10000 / tables[alias_to_table[first_table]].row_number
    # 通过边逐渐求出其他表的采样概率(不太靠谱，有的直接变成0了
    # update_exist = True
    # while update_exist:
    #     update_exist = False
    #     for join in joins:
    #         x = join[0][0]
    #         y = join[1][0]
    #         if P[x] == 0:
    #             x, y = y, x
    #         if P[x] != 0 and P[y] == 0:
    #             update_exist = True
    #             P[y] = P[x] ** (U[y] / U[x])
    for alias in join_tables:
        print(alias, U[alias], P[alias], tables[alias_to_table[alias]].row_number,
              round(tables[alias_to_table[alias]].row_number * P[alias]))

    # 为每个等价类制定一个hash函数
    H = {}
    for eq_cls in equal_class.keys():
        MOD = 1000000007
        H[eq_cls] = (random.randint(1, MOD), random.randint(1, MOD), MOD)
    # 采样得到Samples
    S = defaultdict(list)
    for alias in join_tables:
        table = alias_to_table[alias]
        values = pd.read_csv(f'data/{table}.csv', names=tables[table].columns).values
        for value in values:
            flag = True
            # 要保证每个attr都<一定的概率才被采样
            for attr in u[alias]:
                if attr != 'company_type_id':
                    a, b, p = H[get_father((alias, attr))]
                    ind = tables[table].column_ind[attr]
                    if value[ind]:
                        if type(value[ind]) == int:
                            hv = 1.0 * (a * value[ind] + b) % p / p
                        else:
                            hv = 1.0 * (a * hash(value[ind] + b) % p) / p
                    else:
                        hv = 2
                    if hv >= P[alias] ** (1.0 / U[alias]):
                        flag = False
                        break
            # if alias == 'mc':
            #    print(flag, hv, P[alias], U[alias], P[alias] ** (1.0 / U[alias]), len(values), attr)
            if flag:
                S[alias].append(value)
        record_time(f'Sample {alias} finished, sample {len(S[alias])} rows')

    # 得到采样之后join的结果大小 J
    compare_columns = defaultdict(list)
    for join in joins:
        compare_columns[join[0]].append(join[1])
        compare_columns[join[1]].append(join[0])

    global J
    J = 0
    row = {}

    def dfs(ind):
        if ind == len(join_tables):
            global J
            J += 1
            return
        alias = join_tables[ind]
        table = alias_to_table[alias]
        for value in S[alias]:
            if tables[table].satisfy(value, selections[alias]):
                flag = True
                for attr in u[alias]:
                    for compare_column in compare_columns[(alias, attr)]:
                        if compare_column in row.keys():
                            if value[tables[table].column_ind[attr]] != row[compare_column]:
                                flag = False
                                break
                    if not flag:
                        break
                if flag:
                    for attr in u[alias]:
                        row[(alias, attr)] = value[tables[table].column_ind[attr]]
                    dfs(ind + 1)
        for attr in u[alias]:
            if (alias, attr) in row.keys():
                row.pop((alias, attr))

    dfs(0)
    print(f'Sample join answer is {J}')
    # 计算得到Pinc
    Pinc = 1
    for eq_cls in equal_class:
        mi = 1
        for attr in join_attrs:
            if get_father(attr) == eq_cls:
                mi = min(mi, P[alias] ** (1.0 / U[alias]))
        Pinc *= mi
    return round(J / Pinc)


if __name__ == '__main__':
    init()
    record_time('Init all things')
    data = {}
    for sql_file in ['test']:  # 'middle','easy']:
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
