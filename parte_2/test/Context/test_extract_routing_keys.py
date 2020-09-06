from os.path import sep

from Context import extract_routing_keys

def test_extract_routing_keys_full_path():
    result = extract_routing_keys(sep.join(['a', 'b', 'c.d.e']))
    assert result[0] == 'c'
    assert result[1] == 'd'

def test_extract_routing_keys_just_filename():
    result = extract_routing_keys('c.d.e')
    assert result[0] == 'c'
    assert result[1] == 'd'

def test_extract_routing_keys_simple_string():
    result = extract_routing_keys('c')
    assert result == []

def test_extract_routing_keys_empty_string():
    result = extract_routing_keys('')
    assert result == []
