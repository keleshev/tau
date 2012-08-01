from datetime import datetime


def test_get_one_value(tau):
    tau.set(foo=123)
    assert tau.get('foo') == 123


def test_get_compound_value(tau):
    tau.set(foo={'foo': True, 'bar': None})
    assert tau.get('foo') == {'foo': True, 'bar': None}


def test_set_dict(tau):
    tau.set({'foo': 123})
    assert tau.get('foo') == 123


def test_get_two_values(tau):
    tau.set(foo=123, bar=True)
    assert tau.get('foo', 'bar') == {'foo': 123, 'bar': True}


def test_clear_one_value(tau):
    tau.set(foo=123)
    assert tau.get('foo') == 123
    tau.clear()
    assert tau.get('foo') == None


def test_clear_two_values(tau):
    tau.set(foo=123, bar=True)
    assert tau.get('foo', 'bar') == {'foo': 123, 'bar': True}
    tau.clear()
    assert tau.get('foo', 'bar') == {'foo': None, 'bar': None}


def test_get_wilecard(tau):
    tau.set(foo=123, bar=True)
    assert tau.get('foo', 'bar') == {'foo': 123, 'bar': True}
    assert tau.get('*') == {'foo': 123, 'bar': True}


def test_get_pattern(tau):
    tau.set(meanSpeed=123, meanPower=456, foo=False)
    assert tau.get('mean*') == {'meanSpeed': 123, 'meanPower': 456}


def test_get_pattern_that_matches_nothing(tau):
    tau.set(foo=123, bar=True)
    assert tau.get('?') == {}


def test_get_complex_pattern(tau):
    tau.set(meanSpeed=1, meanPower=2, rpm1=3, rpm2=4, foo=0)
    message = {'meanSpeed': 1, 'meanPower': 2, 'rpm1': 3, 'rpm2': 4}
    assert tau.get('mean*', 'rpm?') == message


def test_get_pattern_and_name(tau):
    tau.set(meanSpeed=1, meanPower=2, rpm1=3, rpm2=4, foo=0)
    message = {'meanSpeed': 1, 'rpm1': 3, 'rpm2': 4}
    assert tau.get('rpm[12]', 'meanSpeed') == message


def test_gets_latest_value(tau):
    tau.set(a=0, b=1, foo=-1)
    tau.set(a=5, b=6, foo=-2)
    tau.set(a=8, b=9, foo=-3)
    assert tau.get('?') == {'a': 8, 'b': 9}


# Signals


def test_signals(tau):
    tau.clear()
    tau.set(a=0, b=1, foo=-1)
    tau.set(a=5, b=6, foo=-2)
    tau.set(a=8, b=9, foo=-3)
    assert set(tau.signals()) == set(['a', 'b', 'foo'])


#def test_matching_signals(tau):
#    tau.set(a=0, b=1, foo=-1)
#    tau.set(a=5, b=6, foo=-2)
#    tau.set(a=8, b=9, foo=-3)
#    assert tau._matching_signals('?') == set(['a', 'b'])
#    assert tau._matching_signals('a', '???') == set(['a', 'foo'])


#def test_internal_get(tau):
#    tau.set(a=5, b=6, foo=-2)
#    tau.set(a=8, b=9, foo=-3)
#    d = tau._get(['a'], =1)
#    assert d.keys() == ['a']
#    [[t1, n], [t2, m]] = d['a']
#    assert type(t1) == type(t2) == datetime
#    assert n == 5 and m == 8
#    assert tau._get(['x'], period=1) == {'x': []}




# Period


def test_get_period(tau):
    tau.clear()
    tau.set(a=0, b=1, foo=-1)
    tau.set(a=5, b=6, foo=-2)
    tau.set(a=8, b=9, foo=-3)
    assert tau.get('?', period=1) == {'a': [0, 5, 8], 'b': [1, 6, 9]}


# Timestamps


def test_get_timestamps(tau):
    tau.set(a=2, b=3)
    tau.set(a=8, b=9)
    d = tau.get('?', timestamps=True)  # {'a': [t, 8], 'b': [t, 9]}
    assert len(d['a']) == 2
    assert type(d['a'][0]) == datetime
    assert d['a'][1] == 8
    assert len(d['b']) == 2
    assert type(d['b'][0]) == datetime
    assert d['b'][1] == 9


def test_get_timestamps_with_period(tau):
    tau.clear()
    tau.set(a=2, b=3)
    tau.set(a=8, b=9)
    # {'a': [[t, 2], [t, 8]], 'b': [[t, 3], [t, 9]]}
    d = tau.get('?', timestamps=True, period=1)
    [[t1, v1], [t2, v2]] = d['a']
    assert t1 < t2
    assert type(t1) == type(t2) == datetime
    assert v1 == 2 and v2 == 8
    [[t1, v1], [t2, v2]] = d['b']
    assert t1 < t2
    assert type(t1) == type(t2) == datetime
    assert v1 == 3 and v2 == 9
