# coding:utf-8
from pandas import DataFrame, api
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer


@api.extensions.register_dataframe_accessor('pdsql')
class PdSQLAccessor():
    def __init__(self, pandas_obj):
        self.df = pandas_obj

    def mapping_df_types(self):
        dtypedict = {}
        for i, j in zip(self.df.columns, self.df.dtypes):
            if "object" in str(j):
                dtypedict.update({i: NVARCHAR(length=255)})
            if "float" in str(j):
                dtypedict.update({i: Float(precision=2, asdecimal=True)})
            if "int" in str(j):
                dtypedict.update({i: Integer()})
        return dtypedict

    def to_sql(self, table, con, index=False, chunksize=10 ** 5, primary_key=None, **kwargs):
        if self.df.empty:
            print('DataFrame is empty!')
            return

        if primary_key and primary_key not in self.df.columns:
            raise Exception(f"Key column '{primary_key}' doesn't exist in table")

        # default data type
        dtype = self.mapping_df_types()
        if 'dtype' in kwargs:
            dtype = dtype.update(kwargs.pop('dtype'))

        # call pandas to_sql function
        self.df.to_sql(table, con, index=index, chunksize=chunksize, dtype=dtype, **kwargs)

        # set primary key
        with con.connect() as con:
            set_primary_sql = f"SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name='{table}' AND constraint_name='PRIMARY'"
            ret = list(con.execute(set_primary_sql))
            cur_primary_keys = ret[0].values() if ret else []
            if primary_key and primary_key not in cur_primary_keys:
                con.execute(f'ALTER TABLE `{table}` ADD PRIMARY KEY (`{primary_key}`);')

    def insert(self, table, con, conflict='fail', limit=10 ** 5):
        if self.df.empty:
            print('DataFrame is empty!')
            return

        if conflict == 'replace':
            prefix = 'REPLACE INTO ' + table
        elif conflict == 'ignore':
            prefix = 'INSERT IGNORE INTO ' + table
        elif not conflict or conflict == 'fail':
            prefix = 'INSERT INTO ' + table
        else:
            raise Exception('Conflict type is not supported, It just can be `replace`, `ignore` or `fail`!')

        source = self.df.to_dict(orient='records')
        fields = source[0].keys()
        values = [one.values() for one in source]

        fields_str = ' (' + ','.join(fields) + ') '
        values_str = ' (' + ','.join(['%s'] * len(fields)) + ') '

        sql = prefix + fields_str + 'VALUES' + values_str

        with con.connect() as con:
            for index in range(0, len(values), limit):
                insert_values = values[index:index + limit]
                ret = con.execute(sql, insert_values)
                print(f'Insert {len(insert_values)} into {table}, {ret.rowcount} rows effected')

        return True

    def update(self, table, con, condition, limit=10 ** 5):

        source = self.df.to_dict(orient='records')

        condition = [condition] if isinstance(condition, str) else condition

        # sort the columns
        columns = source[0].keys()
        set_columns = list(set(columns) - set(condition))
        columns = set_columns + condition

        set_str = ','.join([c + '=%s' for c in set_columns])
        condition_str = ' AND '.join([c + '=%s' for c in condition])

        values = [[dct[column] for column in columns] for dct in source]

        sql = 'UPDATE ' + table + ' SET ' + set_str + ' WHERE ' + condition_str

        with con.connect() as con:
            for index in range(0, len(values), limit):
                insert_values = values[index:index + limit]
                ret = con.execute(sql, insert_values)
                print(f'Insert {len(insert_values)} into {table}, {ret.rowcount} rows effected')

        return True


if __name__ == '__main__':
    # how to use it in your work
    from datetime import datetime

    now = datetime.now().__str__()
    source = [
        {
            'id': 1211,
            'task': 'xxtaa1',
            'reason': now,
        },
        {
            'id': 1111,
            'task': 'xxtask2',
            'reason': now,
        },
        {
            'id': 1114,
            'task': 'xxtasa3',
            'reason': now,
        },
    ]
    df = DataFrame(source)
    engine = create_engine("mysql://root:root@localhost:3306/test?charset=utf8")
    # df.pdsql.to_sql('myTable', engine, primary_key='id', if_exists='append')
    # df.pdsql.insert('myTable', engine, conflict='ignore')
    df.pdsql.update('myTable', engine, condition=['id', 'task'])

    # TODO Using Transactions
