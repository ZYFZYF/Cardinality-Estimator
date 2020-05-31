import pandas as pd

SINGLE_SAMPLE_SIZE = 10000


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
        df = pd.read_csv(f'data/{table_name}.csv', names=self.columns)
        self.sample_values = df.sample(n=min(len(df), SINGLE_SAMPLE_SIZE)).values
        self.row_number = len(df)

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
