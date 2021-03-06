import os
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


def seconds(n):
    return timedelta(seconds=n)


def now():
    return datetime.now()


t = now()  # some timestamp; dosen't matter


@backends(*all)
def test_backend_signals(backend):
    backend.set('foo', t, 9)
    assert backend.signals() == ['foo']
    backend.set('bar', t, 9)
    assert set(backend.signals()) == set(['foo', 'bar'])


@backends(*all)
def test_backend_clear(backend):
    backend.set('eggs', t, 9)
    backend.set('spam', t, 9)
    assert set(backend.signals()) == set(['eggs', 'spam'])
    backend.clear()
    assert set(backend.signals()) == set()


@backends(*all)
def test_backend_get(backend):
    backend.set('foo', t, 8)
    [res] = backend.get('foo')
    assert res[1] == 8
    assert type(res[0]) == datetime


@backends(MemoryBackend, CSVBackend)
def test_backend_get_compound(backend):
    backend.set('foo', t, {'this': 1, 'that': [2, 3]})
    [res] = backend.get('foo')
    assert res[1] == {'this': 1, 'that': [2, 3]}
    assert type(res[0]) == datetime


@backends(*all)
def test_backend_get_start_end(backend):
    backend.set('foo', t, 1)
    backend.set('foo', t, 2)
    backend.set('foo', t, 3)
    one, two, three = backend.get('foo', now() - seconds(1), now())
    assert (one[1], two[1], three[1]) == (1, 2, 3)


@backends(*all)
def test_backend_get_limit(backend):
    for n in range(10):
        backend.set('foo', t, n)
    res = backend.get('foo', now() - seconds(1), now())
    assert len(res) == 10
    res = backend.get('foo', now() - seconds(1), now(), limit=4)
    assert len(res) == 4


def test_memory_backend_errors():
    backend = MemoryBackend(1)  # seconds
    backend.set('key', t, 'value')
    [[time, value]] = backend.get('key',
            start=datetime.now() - timedelta(seconds=0.5),
            end=datetime.now())
    assert type(time) is datetime
    assert value == 'value'
    with raises(BackendError):
        print(backend.get('key',
            start=datetime.now() - timedelta(seconds=1.5),
            end=datetime.now()))


def test_binary_backend_errors():
    backend = BinaryBackend()
    backend.set('key', t, 1)
    backend.set('key', t, '1')
    with raises(BackendError):
        backend.set('key', t, 'I')


def test_glue_backend_dispatch():
    mem = MemoryBackend(1)
    bin = BinaryBackend()
    csv = CSVBackend()
    glue = GlueBackend(mem, bin, csv)
    glue.set('key', t, 1)
    assert glue.get('key')[0][1] == 1
    assert glue.get('key') == mem.get('key') == csv.get('key')
    glue.clear()
    glue.set('key', t, 'value')
    assert glue.get('key')[0][1] == 'value'
    assert glue.get('key') == mem.get('key') == csv.get('key')
    assert bin.get('key') == []


def test_glue_failure():
    glue = GlueBackend(BinaryBackend())
    glue.set('key', t, 1)
    with raises(BackendError):
        glue.set('key', t, 'I')


def test_glue_tries_to_get_from_other_backends_if_gets_empty_list():
    mem = MemoryBackend(9)
    csv = CSVBackend()
    glue = GlueBackend(mem, csv)
    glue.set('key', t, 9)
    mem.clear()  # say, machine was rebooted, but csv files are still there
    assert glue.get('key')[0][1] == 9


@backends(CSVBackend, BinaryBackend)
def test_file_backends_dont_fail_if_file_is_empty(backend):
    os.system('touch hai.csv hai.TIME hai.VALUE')
    assert backend.get('hai') == []
