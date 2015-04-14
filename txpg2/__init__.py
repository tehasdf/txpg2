
from functools import wraps
from twisted.internet.protocol import Protocol
import struct
from twisted.protocols.stateful import StatefulProtocol


def handler(f):
    @wraps(f)
    def inner(self, data):
        f(self, data)
        return self.getInitialState()
    return inner

class PostgresProtocol(StatefulProtocol):
    def getInitialState(self):
        return (self.getHeader, 5)

    def getHandler(self, tag):
        handlers = {
            'R': self.handle_Authentication
        }
        if tag in handlers:
            return handlers[tag]
        else:
            print 'UNKNOWN TAG %r' % (tag, )
            return self.handle_Unknown

    def getHeader(self, header):
        tag, length = struct.unpack('!cI', header)
        handler = self.getHandler(tag)
        return handler, length - 4

    def connectionMade(self):
        print 'connected'
        self.send_StartupMessage()

    def send(self, tag, payload):
        if tag is None:
            tag = ''

        length = len(payload) + 4
        length_packed = struct.pack('!I', length)
        data = ''.join([tag, length_packed, payload])
        self.transport.write(data)

    def send_StartupMessage(self):
        payload = {
            'user': 'asdf',
            'database': 'asdf'
        }
        data = struct.pack('!hh', 3, 0)
        for k, v in payload.items():
            data += '%s\x00%s\x00' % (k, v)
        data += '\x00'

        self.send(None, data)

    @handler
    def handle_Authentication(self, data):
        print 'len', len(data)
        if len(data) == 8:
            print 'md5 auth required'
        elif len(data) == 4:
            print 'r4'


    @handler
    def handle_Unknown(self, data):
        print 'unknown handler, data: %r' % (data, )