import sqlite3

import pytest

from Context import QuoteOrderContext

@pytest.fixture
def context():
    context = QuoteOrderContext('test/extra/data/quote.order.json')
    return context

def test_order(context):
    df_order = context.transformation()
    list_data = df_order.toPandas().to_dict('records')

    assert list_data[0]['id'] == 'be72ca71-7c3b-5e82-abc6-99e2b4c01852'
    assert list_data[1]['id'] == '97973cee-9d54-54a3-be64-bd4900f7dea1'
    assert list_data[2]['id'] == 'd2bca186-63f9-584c-8bc4-4119a31f5650'

    assert list_data[0]['pricing'] == 23.61
    assert list_data[1]['pricing'] == 73.14
    assert list_data[2]['pricing'] == 82.11

    assert list_data[0]['updated_at'] == '2018-03-16 14:19:48'
    assert list_data[1]['updated_at'] == '2018-03-16 15:19:58'
    assert list_data[2]['updated_at'] == '2018-03-17 14:19:58'
