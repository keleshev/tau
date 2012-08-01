#! /usr/bin/env python
"""tau: time series database

       .##########
      ############
     #    :##
          ##,
          ##,   ,
          ###__#
          `####'

Usage:
  tau (-h | --help | --version)
  tau server
      [-a <host:port>]
  tau set <key=value> ...
      [-a <host:port>]
  tau get <key> ... [--timestamps] [--period=<seconds>]
      [-a <host:port>]
  tau clear
      [-a <host:port>]

Options:
  -a, --address <host:port>  TCP address [default: localhost:6283].

"""
import socket
import os
import json
from fnmatch import fnmatchcase
from datetime import datetime, timedelta

from docopt import docopt


class TauProtocol(object):

    def __init__(self, host='localhost', port=6283, client=None):
        self._host = host
        self._port = port
        self._client = client

    def __enter__(self):
        if not self._client:
            self._client = socket.socket()
            self._client.connect((self._host, self._port))
        return self

    def send(self, message):
        def encode_datetime(obj):
            if type(obj) is datetime:
                return {'__datetime__': obj.isoformat()}
            raise TypeError("%r is not JSON serializable" % obj)
        #assert '\n' not in json.dumps(message)
        self._client.send(json.dumps(message, default=encode_datetime) + '\n')

    def receive(self):
        def decode_datetime(obj):
            if '__datetime__' in obj:
                return datetime.strptime(obj['__datetime__'],
                                         '%Y-%m-%dT%H:%M:%S.%f')
            return obj
        message = ''
        while True:
            message += self._client.recv(4096)
            if message.endswith('\n'): # or message == '':
                break
        return json.loads(message, object_hook=decode_datetime)

    def __exit__(self, exception_type, value, traceback):
        #self._client.shutdown(socket.SHUT_RDWR)
        self._client.close()


class TauServer(object):

    def __init__(self, host='localhost', port=6283, cache_seconds=1):
        try:
            self.tau = Tau()#cache_seconds)
            self.server = socket.socket()
            #self.server.bind((socket.gethostname(), port))
            self.server.bind((host, port))
            self.server.listen(5)
            while True:
                client, address = self.server.accept()
                with TauProtocol(client=client) as protocol:
                    command, argument = protocol.receive()
                    if command == 'get':
                        protocol.send(self.tau.get(*argument[0],
                                                   **argument[1]))
                    elif command == 'set':
                        self.tau.set(argument)
                    elif command == 'signals':
                        protocol.send(self.tau.signals())
                    elif command == 'clear':
                        self.tau.clear()
        finally:
            #self.server.shutdown(socket.SHUT_RDWR)
            self.server.close()


class TauClient(object):

    def __init__(self, host='localhost', port=6283):
        self._host = host
        self._port = port

    def set(self, *arg, **kw):
        data = arg[0] if arg else kw
        with TauProtocol(self._host, self._port) as protocol:
            protocol.send(['set', data])

    def get(self, *arg, **kw):
        with TauProtocol(self._host, self._port) as protocol:
            protocol.send(['get', [arg, kw]])
            return protocol.receive()

    def signals(self):
        with TauProtocol(self._host, self._port) as protocol:
            protocol.send(['signals', None])
            return protocol.receive()

    def clear(self):
        with TauProtocol(self._host, self._port) as protocol:
            protocol.send(['clear', None])


class MemoryBackend(object):

    def __init__(self, cache_seconds):
        self._state = {}
        self._cache_seconds = cache_seconds

    def set(self, key, value):
        if key not in self._state:
            self._state[key] = []
        self._state[key].append([datetime.now(), value])
        self._state = self._truncate(self._state, self._cache_seconds)

    def get(self, signal, start=None, end=None):
        self._state = self._truncate(self._state, self._cache_seconds)
        if signal not in self._state or self._state[signal] == []:
            return [] if start and end else None
        if start and end:
            return [kv for kv in self._state[signal] if start <= kv[0] <= end]
        return self._state[signal][-1]

    def signals(self):
        return self._state.keys()

    @staticmethod
    def _truncate(state, period):
        now = datetime.now()
        for key in state:
            state[key] = [[t, v] for [t, v] in state[key]
                          if (now - t).total_seconds() < period]
        return state

    def clear(self):
        self._state = {}


class CSVBackend(object):

    def __init__(self, path='./'):
        self._path = path

    def set(self, key, value):
        with open(self._path + key + '.csv', 'a') as f:
            f.write('%s,%s\n' % (datetime.now().isoformat(), json.dumps(value)))

    def get(self, signal, start=None, end=None):
        if signal not in self.signals():
            return [] if start and end else None
        if start and end:
            result = []
            with open(self._path + signal + '.csv') as f:
                for line in f.xreadlines():
                    t, _, v = line.partition(',')
                    t = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%f')
                    if start <= t <= end:
                        v = json.loads(v.strip())
                        result.append([t, v])
            return result
        with open(self._path + signal + '.csv') as f:
            for line in f.xreadlines():
                t, _, v = line.partition(',')
            t = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%f')
            v = json.loads(v.strip())
        return [t, v]


    def signals(self):
        return [f[:-4] for f in os.listdir(self._path) if f.endswith('.csv')]

    def clear(self):
        [os.remove(self._path + f) for f in os.listdir(self._path)
         if f.endswith('.csv')]


class Tau(object):

    def __init__(self, backend=MemoryBackend(cache_seconds=1)):
        self._backend = backend

    def __repr__(self):
        return 'Tau(%r)' % self._backend

    def set(self, *arg, **kw):
        keyvalues = arg[0] if arg else kw
        for key, value in keyvalues.items():
            self._backend.set(key, value)

    def get(self, *arguments, **options):
        signals = self._matching_signals(*arguments)

        if 'period' in options or 'start' in options and 'end' in options:
            if 'period' in options:
                end = datetime.now()
                start = end - timedelta(seconds=options['period'])
            else:
                end = options['end']
                start = options['start']
            match = dict((s, self._backend.get(s, start, end)) for s in signals)
            if 'timestamps' not in options:
                match = dict((k, [i[1] for i in v]) for k, v in match.items())
        else:  # latest value
            match = dict((s, self._backend.get(s)) for s in signals)
            if 'timestamps' not in options:
                match = dict((k, v[1] if v else None) for k, v in match.items())

        if len(arguments) == 1 and not self._is_pattern(arguments[0]):
            return match[arguments[0]]
        return match

    def _matching_signals(self, *arg):
        patterns = [a for a in arg if self._is_pattern(a)]
        signals = [a for a in arg if not self._is_pattern(a)]
        return set([s for p in patterns for s in self._backend.signals()
                    if fnmatchcase(s, p)] + signals)

    @staticmethod
    def _is_pattern(s):
        return '*' in s or '?' in s or '[' in s or ']' in s

    def signals(self):
        return self._backend.signals()

    def clear(self):
        self._backend.clear()


if __name__ == '__main__':
    args = docopt(__doc__, version='zero')
    host, port = args['--address'].split(':')
    tau = TauClient(host=host, port=int(port))
    if args['server']:
        try:
            TauServer(host=host, port=int(port))
        except KeyboardInterrupt:
            pass
    elif args['set']:
        tau.set(dict(kv.split('=') for kv in args['<key=value>']))
    elif args['get']:
        print(tau.get(*args['<key>'], period=args['--period'],
                      timestamps=args['--timestamps']))
    elif args['clear']:
        tau.clear()
