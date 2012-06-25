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
  tau server [--port=<port>]
  tau set <key=value> ...
      [--host=<host>] [--port=<port>]
  tau get <key> ... [--timestamps] [--period=<seconds>]
      [--host=<host>] [--port=<port>]
  tau clear
      [--host=<host>] [--port=<port>]

Options:
  --host=<host>  [default: localhost]
  --port=<port>  [default: 6283]

"""
import socket
import json
from fnmatch import fnmatchcase
from datetime import datetime

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

    def __init__(self, port=6283, lifetime=1):
        try:
            self.tau = Tau(lifetime)
            self.server = socket.socket()
            #self.server.bind((socket.gethostname(), port))
            self.server.bind(('', port))
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

    def clear(self):
        with TauProtocol(self._host, self._port) as protocol:
            protocol.send(['clear', None])


class Tau(object):

    def __init__(self, lifetime=1):
        self._lifetime = lifetime
        self._state = {}

    def set(self, *arg, **kw):
        keyvalues = arg[0] if arg else kw
        for key, value in keyvalues.items():
            if key not in self._state:
                self._state[key] = []
            self._state[key].append([datetime.now(), value])

    def get(self, *arg, **kw):
        self._state = self._truncate(self._state, self._lifetime)
        match = self._get_match(arg)
        if kw.get('period'):
            match = self._truncate(match, kw['period'])

        if kw.get('period') and kw.get('timestamps'):
            transform = lambda x: x if x else []
        elif kw.get('period'):
            transform = lambda x: list(zip(*x)[1]) if x else []
        elif kw.get('timestamps'):
            transform = lambda x: x[-1] if x else []
        else:
            transform = lambda x: x[-1][1] if x else None

        match = dict((key, transform(val)) for key, val in match.items())

        if len(arg) == 1 and not self._is_pattern(arg[0]):
            return match.get(arg[0])
        return match

    def _get_match(self, argument):
        patterns = [a for a in argument if self._is_pattern(a)]
        keys = [a for a in argument if not self._is_pattern(a)]
        patterns_match = {}
        for p in patterns:
            for k in self._state.keys():
                if fnmatchcase(k, p):
                    patterns_match[k] = self._state.get(k)
        keys_match = dict((key, self._state.get(key)) for key in keys)
        return dict(patterns_match.items() + keys_match.items())

    @staticmethod
    def _truncate(state, period):
        now = datetime.now()
        for key in state:
            state[key] = [[t, v] for [t, v] in state[key]
                          if (now - t).total_seconds() < period]
        return state

    @staticmethod
    def _is_pattern(s):
        return '*' in s or '?' in s or '[' in s or ']' in s

    def clear(self):
        self._state = {}


if __name__ == '__main__':
    args = docopt(__doc__, version='zero')
    tau = TauClient(host=args['--host'], port=int(args['--port']))
    if args['server']:
        TauServer(port=int(args['--port']))
    elif args['set']:
        tau.set(dict(kv.split('=') for kv in args['<key=value>']))
    elif args['get']:
        print(tau.get(*args['<key>'], period=args['--period'],
                      timestamps=args['--timestamps']))
    elif args['clear']:
        tau.clear()
