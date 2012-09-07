from datetime import datetime, timedelta

from pytest import raises, mark

from tau import MemoryBackend, BinaryBackend, CSVBackend, GlueBackend
from tau import BackendError


glue_backend = lambda: GlueBackend(MemoryBackend(), CSVBackend())
all = (MemoryBackend, BinaryBackend, CSVBackend, glue_backend)


def backends(*backends):
    return mark.parametrize(('backend',), [(b(),) for b in backends])


def teardown_function(function):
    for backend in all:
        backend().clear()


def now():
    return datetime.now()


def seconds(n):
    return timedelta(seconds=n)



@backends(*all)
def test_backend_signals(backend):
    backend.set('foo', 9)
    assert backend.signals() == ['foo']
    backend.set('bar', 9)
    assert set(backend.signals()) == set(['foo', 'bar'])


@backends(*all)
def test_backend_clear(backend):
    backend.set('eggs', 9)
    backend.set('spam', 9)
    assert set(backend.signals()) == set(['eggs', 'spam'])
    backend.clear()
    assert set(backend.signals()) == set()


@backends(*all)
def test_backend_get(backend):
    backend.set('foo', 8)
    [res] = backend.get('foo')
    assert res[1] == 8
    assert type(res[0]) == datetime


@backends(MemoryBackend, CSVBackend)
def test_backend_get_compound(backend):
    backend.set('foo', {'this': 1, 'that': [2, 3]})
    [res] = backend.get('foo')
    assert res[1] == {'this': 1, 'that': [2, 3]}
    assert type(res[0]) == datetime


@backends(*all)
def test_backend_get_start_end(backend):
    backend.set('foo', 1)
    backend.set('foo', 2)
    backend.set('foo', 3)
    one, two, three = backend.get('foo', now() - seconds(1), now())
    assert (one[1], two[1], three[1] == 1, 2, 3)


@backends(*all)
def test_backend_get_limit(backend):
    for n in range(10):
        backend.set('foo', n)
    res = backend.get('foo', now() - seconds(1), now())
    assert len(res) == 10
    res = backend.get('foo', now() - seconds(1), now(), limit=4)
    assert len(res) == 4


def test_backend_error():
    backend = MemoryBackend(1)  # seconds
    backend.set('key', 'value')
    [[time, value]] = backend.get('key',
            start=datetime.now() - timedelta(seconds=0.5),
            end=datetime.now())
    assert type(time) is datetime
    assert value == 'value'
    with raises(BackendError):
        print(backend.get('key',
            start=datetime.now() - timedelta(seconds=1.5),
            end=datetime.now()))
