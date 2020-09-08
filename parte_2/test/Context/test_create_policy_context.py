import sqlite3

import pytest

from Context import CreatePolicyContext

@pytest.fixture
def context():
    context = CreatePolicyContext('test/extra/data/create.policy.json')
    return context

def test_policy(context):
    df_policy = context.transformation()
    list_data = df_policy.toPandas().to_dict('records')

    assert 'id' in df_policy.columns

    assert list_data[0]['id'] == 1000075771401371
    assert list_data[1]['id'] == 1000041110339228

    assert list_data[0]['order_id'] == '4472ef6b-df63-594f-a70a-9cca68dfda09'
    assert list_data[1]['order_id'] == '3c8bebd4-9b36-569c-a9fa-0ab0e71bab9f'

    assert list_data[0]['insurance_type'] == 'life'
    assert list_data[1]['insurance_type'] == 'life'

    assert list_data[0]['status'] == 'created'
    assert list_data[1]['status'] == 'created'

    assert list_data[0]['created_at'] == '2018-03-16 14:23:08'
    assert list_data[1]['created_at'] == '2018-03-16 15:23:18'
