import sqlite3

import pytest

from Context import UpdatePolicyContext

def test_policy_update_no_reason():
    context = UpdatePolicyContext('test/extra/data/activate.policy.json',
                                  'activated')

    df_policy = context.transformation()
    list_data = df_policy.toPandas().to_dict('records')

    assert list_data[0]['id'] == 1000032024263999
    assert list_data[0]['status'] == 'activated'
    assert list_data[0]['updated_at'] == '2018-03-16 14:26:28'

def test_policy_update_with_reason():
    context = UpdatePolicyContext('test/extra/data/cancel.policy.json',
                                  'cancelled')
    df_policy = context.transformation()
    list_data = df_policy.toPandas().to_dict('records')

    assert list_data[0]['id'] == 1000053205442708
    assert list_data[0]['status'] == 'cancelled'
    assert list_data[0]['reason'] == 'Estou insatisfeito(a) com os serviços da Youse.'
    assert list_data[0]['updated_at'] == '2018-05-10 19:13:18'
