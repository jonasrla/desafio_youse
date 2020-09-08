import sqlite3

import pytest

from Context.base_context import BaseContext

@pytest.fixture
def ConcreteContext():
    class ConcreteContext(BaseContext):
        def __init__(self, file_path):
            self.app_name = 'testing'
            super().__init__(file_path)
            self.db_path = 'test/extra/datalake.db'

        def process(self):
            pass

        def transformation(self):
            pass

    yield ConcreteContext

    with sqlite3.connect('test/extra/datalake.db') as conn:
        cur = conn.cursor()
        cur.execute('DELETE FROM test')
        conn.commit()


def test_constructor(ConcreteContext):
    context = ConcreteContext('test/extra/data/append.json')
    df_data = context.input
    list_data = df_data.toPandas().to_dict('records')
    assert list_data[0]['id'] == 'a'
    assert list_data[1]['id'] == 'b'
    assert list_data[2]['id'] == 'c'
    assert list_data[0]['column'] == '1'
    assert list_data[1]['column'] == '2'
    assert list_data[2]['column'] == '3'

def test_append_table(ConcreteContext):
    context = ConcreteContext('test/extra/data/append.json')
    df_data = context.input
    assert context.append_table(df_data, 'test') == 'Success'

def test_append_table_invalid_schema(ConcreteContext):
    context = ConcreteContext('test/extra/data/append.json')
    df_data = context.input
    assert context.append_table(df_data.withColumnRenamed('column',
                                                          'other_column'),
                                                   'test') == 'Error'

def test_update_table(ConcreteContext):
    context = ConcreteContext('test/extra/data/update.json')
    df_data = context.input
    assert context.update_table(df_data, 'test') == 'Success'

def test_update_table_invalid_schema(ConcreteContext):
    context = ConcreteContext('test/extra/data/update.json')
    df_data = context.input
    assert context.update_table(df_data.withColumnRenamed('column',
                                                          'other_column'),
                                'test') == 'Error'
