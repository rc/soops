nested_dict = {'a': {'b': {'c': 1, 'd': 2}, 'c': 3}, 'b': 4}
flat_dict = {'x-a__b__c': 1, 'x-a__b__d': 2, 'x-a__c': 3, 'x-b': 4}

def test_flatten_dict():
    from soops import flatten_dict

    out = flatten_dict(nested_dict, prefix='x-')
    assert out == flat_dict

def test_unflatten_dict():
    from soops import unflatten_dict

    out = unflatten_dict(flat_dict, prefix='x-')
    assert out == nested_dict
