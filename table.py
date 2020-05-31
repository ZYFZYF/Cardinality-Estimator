import pandas as pd

SINGLE_SAMPLE_SIZE = 10000


class Table:
    columns = []
    int_columns = []
    sample_values = []
    row_number = 0

    def __init__(self, table_name, column_list):
        self.columns = [column[0] for column in column_list]
        self.int_columns = [column[0] for column in column_list if column[1] == int]
        df = pd.read_csv(f'data/{table_name}.csv', names=self.columns)
        self.sample_values = df.sample(n=min(len(df), SINGLE_SAMPLE_SIZE)).values
        self.row_number = len(df)
        