# coding:utf-8
import re

from pandas import DataFrame, api
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer


@api.extensions.register_dataframe_accessor('pdsql')
class PdSQLAccessor():
    def __init__(self, pandas_obj):
        self.df = pandas_obj

    def mapping_df_types(self, df):
        dtypedict = {}
        for i, j in zip(df.columns, df.dtypes):
            if "object" in str(j):
                dtypedict.update({i: NVARCHAR(length=255)})
            if "float" in str(j):
                dtypedict.update({i: Float(precision=2, asdecimal=True)})
            if "int" in str(j):
                dtypedict.update({i: Integer()})
        return dtypedict

    def to_sql(self, name, con, index=False, chunksize=10 ** 5, primary_key=None, **kwargs):
        if self.df.empty:
            print('DataFrame is empty!')
            return

        dtype = kwargs.get('dtype', self.mapping_df_types(self.df))
        self.df.to_sql(name, con, index=index, chunksize=chunksize, dtype=dtype)

        # set primary key
        if primary_key:
            with con.connect() as con:
                con.execute(f'ALTER TABLE `{name}` ADD PRIMARY KEY (`{primary_key}`);')

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

        for index in range(0, len(values), limit):
            insert_values = values[index:index + limit]
            with con.connect() as con:
                ret = con.execute(sql, insert_values)
            print(f'Insert {len(insert_values)} into {table}, {ret.rowcount} rows effected')

        return True

    def update(self):
        pass


if __name__ == '__main__':
    # how to use it in your work
    from datetime import datetime

    now = datetime.now().__str__()
    source = [
        {
            'id': 10,
            'task': 'taa1',
            'reason': now,
        },
        {
            'id': 11,
            'task': 'task2',
            'reason': now,
        },
        {
            'id': 14,
            'task': 'tasa3',
            'reason': now,
        },
    ]
    df = DataFrame(source)
    engine = create_engine("mysql://root:root@localhost:3306/test?charset=utf8")
    # df.pdsql.to_sql('myTable', engine, primary_key='id')
    df.pdsql.insert('myTable', engine, conflict='ignore')
    # TODO remove duplicate entry warning
