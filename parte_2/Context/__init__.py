from os.path import sep

from .create_order_context import CreateOrderContext
from .quote_order_context import QuoteOrderContext
from .activate_policy_context import ActivatePolicyContext
from .cancel_policy_context import CancelPolicyContext
from .create_policy_context import CreatePolicyContext
from .refuse_policy_context import RefusePolicyContext

exception_message = 'Invalid service routing key. Expected {VALID_VALUES} but \
got {CURRENT_VALUE} out of {ROUTING_KEY}'

def extract_routing_keys(file_path):
    filename = file_path.split(sep)[-1]
    return filename.split('.')[:-1]

def get_context(file_path):
    routing_key = extract_routing_keys(file_path)
    if routing_key[0] == 'order-service':
        if routing_key[-2] == 'quote':
            context = QuoteOrderContext(file_path)
        elif routing_key[-1] == 'created':
            context = CreateOrderContext(file_path)
        else:
            raise KeyError(exception_message.format(
                           VALID_VALUES = 'created or quote',
                           CURRENT_VALUE = routing_key[-2],
                           ROUTING_KEY = '.'.join(routing_key)))
    elif routing_key[0] == 'policy-service':
        if routing_key[-1] == 'activated':
            context = ActivatePolicyContext(file_path)
        elif routing_key[-1] == 'cancelled':
            context = CancelPolicyContext(file_path)
        elif routing_key[-1] == 'created':
            context = CreatePolicyContext(file_path)
        elif routing_key[-1] == 'refused':
            context = RefusePolicyContext(file_path)
        else:
            raise KeyError(exception_message.format(
                           VALID_VALUES = 'activated or cancelled or created or\
 refused',
                           CURRENT_VALUE = routing_key[-1],
                           ROUTING_KEY = '.'.join(routing_key)))
    else:
        raise KeyError(exception_message.format(
            VALID_VALUES = 'order-service or policy-service',
            CURRENT_VALUE = routing_key[0],
            ROUTING_KEY = '.'.join(routing_key)))
    return context
