import pandas as pd
import json

SINGLE_SAMPLE_SIZE = 10000
RE_SAMPLE = False


class Table:
    columns = []
    int_columns = []
    column_ind = []
    sample_values = []
    row_number = 0

    def __init__(self, table_name, column_list):
        self.columns = [column[0] for column in column_list]
        self.column_ind = {column: ind for ind, column in enumerate(self.columns)}
        self.int_columns = [column[0] for column in column_list if column[1] == int]
        if RE_SAMPLE:
            df = pd.read_csv(f'data/{table_name}.csv', names=self.columns)
            sample_values = df.sample(n=min(len(df), SINGLE_SAMPLE_SIZE)).values.tolist()
            data = {'sample_values': sample_values, 'row_number': len(df)}
            with open(f'sample/{table_name}.json', 'w') as f:
                json.dump(data, f)
        with open(f'sample/{table_name}.json', 'r') as f:
            data = json.load(f)
            self.sample_values = data['sample_values']
            self.row_number = data['row_number']

    def satisfy(self, row, selections):
        for column in selections.keys():
            value = row[self.column_ind[column]]
            for condition in selections[column]:
                cmp = condition[0]
                term = condition[1]
                if cmp == 'BETWEEM' and not (term[0] <= value <= term[1]):
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
        return 1.0 * count / SINGLE_SAMPLE_SIZE
