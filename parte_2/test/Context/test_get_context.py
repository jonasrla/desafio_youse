import pytest

from Context import get_context

from Context import CreateOrderContext, QuoteOrderContext, \
    ActivatePolicyContext, CancelPolicyContext, CreatePolicyContext, \
    RefusePolicyContext


def test_get_context_order_create():
    context = get_context('order-service.auto.order.created.json')
    assert type(context) is CreateOrderContext

def test_get_context_order_quote():
    context = get_context('order-service.auto.order.quote.created.json')
    assert type(context) is QuoteOrderContext

def test_get_context_policy_activate():
    context = get_context('policy-service.auto.policy.activated.json')
    assert type(context) is ActivatePolicyContext

def test_get_context_policy_cancel():
    context = get_context('policy-service.auto.policy.cancelled.json')
    assert type(context) is CancelPolicyContext

def test_get_context_policy_create():
    context = get_context('policy-service.auto.policy.created.json')
    assert type(context) is CreatePolicyContext

def test_get_context_policy_refuse():
    context = get_context('policy-service.auto.policy.refused.json')
    assert type(context) is RefusePolicyContext

def test_get_context_bad_service():
    with pytest.raises(KeyError):
        get_context('a.auto.order.created.json')

def test_get_context_bad_action():
    with pytest.raises(KeyError):
        get_context('order-service.auto.order.creatd.json')

    with pytest.raises(KeyError):
        get_context('policy-service.auto.order.ac.json')
