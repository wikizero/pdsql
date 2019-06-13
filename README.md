# pdsql
#### Rewrite Pandas to_sql function and support insert, update functions 

**How to use it in your work:**
```python
from pandas import DataFrame, api
from sqlalchemy import create_engine

import pdsql
from datetime import datetime

now = datetime.now().__str__()
source = [
    {
        'id': 1,
        'task': 'task1',
        'reason': now,
    },
    {
        'id': 2,
        'task': 'task2',
        'reason': now,
    },
    {
        'id': 3,
        'task': 'task3',
        'reason': now,
    },
]
df = DataFrame(source)

# create engine
engine = create_engine("mysql://root:root@localhost:3306/test?charset=utf8")

# use to_sql like Pandas to_sql and support primary key
df.pdsql.to_sql('myTable', engine, primary_key='id', if_exists='append')

# insert data to database 
df.pdsql.insert('myTable', engine, conflict='ignore')

# update 
df.pdsql.update('myTable', engine, condition=['id'])
```