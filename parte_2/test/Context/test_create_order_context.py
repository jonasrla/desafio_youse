import sqlite3

import pytest

from Context import CreateOrderContext

@pytest.fixture
def context():
    context = CreateOrderContext('test/extra/data/create.order.json')
    return context


def test_client(context):
    df_client, _ = context.transformation()
    list_data = df_client.toPandas().to_dict('records')

    assert 'id' in df_client.columns

    assert list_data[0]['name'] == 'Lloyd Mills'
    assert list_data[1]['name'] == 'Sue Norman'

    assert list_data[0]['phone'] == '(727) 696-5336'
    assert list_data[1]['phone'] == '(789) 812-1625'

    assert list_data[0]['email'] == 'lev@ave.tm'
    assert list_data[1]['email'] == 'vigi@cogut.hn'


def test_order(context):
    df_client, df_order = context.transformation()
    list_data = df_order.toPandas().to_dict('records')
    list_client_ids = df_client.select('id').toPandas().to_dict('records')

    assert 'id' in df_client.columns

    assert list_data[0]['insurance_type'] == 'auto'
    assert list_data[1]['insurance_type'] == 'auto'

    assert list_data[0]['sales_channel'] == 'callcenter'
    assert list_data[1]['sales_channel'] == 'website'

    assert list_data[0]['client_id'] == list_client_ids[0]['id']
    assert list_data[1]['client_id'] == list_client_ids[1]['id']

    assert list_data[0]['created_at'] == '2018-05-09 15:19:38'
    assert list_data[1]['created_at'] == '2018-05-10 14:19:38'
