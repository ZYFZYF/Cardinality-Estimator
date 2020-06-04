import pandas as pd
import json
from collections import defaultdict

SAMPLE_SIZE = ['1000000', '10000', '5000', '2500', '1000', '500', '200', '100', '50', '10']
RE_SAMPLE = False


class Table:

    def __init__(self, table_name, column_list, join_attrs, H, father):
        self.sample_values = []
        self.U = 0
        self.u = []
        self.columns = [column[0] for column in column_list]
        self.column_ind = {column: ind for ind, column in enumerate(self.columns)}
        self.int_columns = [column[0] for column in column_list if column[1] == int]
        self.table_name = table_name
        for attr in join_attrs[table_name]:
            if father[(table_name, attr)] not in [('movie_companies', 'company_type_id'),
                                                  ('person_info', 'info_type_id'),
                                                  ('movie_link', 'link_type_id'),
                                                  ('title', 'kind_id'),
                                                  ('cast_info', 'role_id'),
                                                  ('complete_cast', 'status_id')]:
                self.U += 1
                self.u.append(attr)
        self.U = max(self.U, 1)
        self.P = {}
        self.sample_values = {}
        if RE_SAMPLE:
            df = pd.read_csv(f'data/{table_name}.csv', names=self.columns)
            values = df.values.tolist()
            self.row_number = len(df)
            for sample_size in SAMPLE_SIZE:
                sample_rate = min(1.0, 1.0 * int(sample_size) / self.row_number)
                if table_name == 'cast_info':
                    sample_rate /= 10
                self.P[sample_size] = sample_rate
                self.sample_values[sample_size] = []
                for value in values:
                    flag = True
                    # 要保证每个attr都<一定的概率才被采样
                    for attr in self.u:
                        a, b, p = H[father[table_name, attr]]
                        ind = self.column_ind[attr]
                        if value[ind]:
                            if type(value[ind]) == int:
                                hv = 1.0 * (a * value[ind] + b) % p / p
                            else:
                                hv = 1.0 * (a * hash(value[ind] + b) % p) / p
                        else:
                            hv = 2
                        if hv >= self.P[sample_size] ** (1.0 / self.U):
                            flag = False
                            break
                    if flag:
                        self.sample_values[sample_size].append(value)
            data = {'row_number': len(df), 'P': self.P, 'sample_values': self.sample_values}
            with open(f'sample/{table_name}.json', 'w') as f:
                json.dump(data, f)
        with open(f'sample/{table_name}.json', 'r') as f:
            data = json.load(f)
            self.sample_values = data['sample_values']
            self.row_number = data['row_number']
            self.P = data['P']
            print(f'Sample {[len(values) for values in self.sample_values.values()]} from table {table_name}')
            print(f"Table {table_name}'U: {self.U}, u: {self.u}")

    def satisfy(self, row, selections):
        for column in selections.keys():
            value = row[self.column_ind[column]]
            for condition in selections[column]:
                cmp = condition[0]
                term = condition[1]
                # if self.table_name == 'movie_info_idx':
                #     print(value, term, value == term)
                if cmp == 'BETWEEN' and not (term[0] <= value <= term[1]):
                    return 0
                if cmp == '<' and not (value < term):
                    return 0
                if cmp == '>' and not (value > term):
                    return 0
                if cmp == '<=' and not (value <= term):
                    return 0
                if cmp == '>=' and not (value >= term):
                    return 0
                if cmp == '!=' and not (value != term):
                    return 0
                if cmp == '=' and not (value == term):
                    return 0
        return 1

    def select(self, selections):
        count = 0
        for row in self.sample_values:
            count += self.satisfy(row, selections)
        return 1.0 * count / SAMPLE_SIZE
