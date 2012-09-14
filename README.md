Tau—time series database
========================

       .##########
      ############
     #`   :##
          ##,
          ##,   ,
          ###__#
          `####'

Start tau server (with memory backend):

```bash
./tau.py server -b memory
```

Use `TauClient` to access the database from python:

```python
from tau import TauClient
tau = TauClient()
```

Send some values:

```python
tau.set(my_key='my_value')
tau.set(my_key=3.1415)
tau.set({'my_key': 6.283})
```

Receive values back (possibly from another process):

```python
assert tau.get('my_key') == 6.283
```

Receive previous values over a period (in seconds):

```python
assert tau.get('my_key', period=30) == ['my_value', 3.1415, 6.283]
```

Receive previous values with their timestamps:

```python
data = tau.get('my_key', period=30, timestamps=True)

assert data == [[datetime(...), 'my_value'],
                [datetime(...), 3.1415],
                [datetime(...), 6.283]]
```

Send some more values, any JSON-serializable values will do:

```python
tau.set(another_key=42, yet_another_key=True)
```

Receive all available values:

```python
assert tau.get('*') == {'my_key': 6.283,
                        'another_key': 42,
                        'yet_another_key': True}
```

Receive values based on a patter, using `*`, `?`, `[abc]`:

```python
assert tau.get('*_key') == {'my_key': 6.283,         
                            'another_key': 42,       
                            'yet_another_key': True} 
assert tau.get('*another_key') == {'another_key': 42,       
                                   'yet_another_key': True} 
```

For more examples see `test_*.py` files.
