
from collections import deque
from functools import wraps
from twisted.internet.protocol import Protocol
import struct
from twisted.protocols.stateful import StatefulProtocol
from twisted.internet.defer import DeferredQueue, Deferred

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
            'R': self.handle_Authentication,
            'S': self.handle_ParameterStatus,
            'K': self.handle_BackendKeyData,
            'Z': self.handle_ReadyForQuery,
            'E': self.handle_ErrorResponse,
            'T': self.handle_RowDescription,
            'D': self.handle_DataRow,
            'C': self.handle_CommandComplete
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
        self.parameters = {}
        self.cancellationKey = None
        self.backendPID = None
        self.transactionStatus = None

        self._dfds = deque()
        self._queries = DeferredQueue()
        self._currentResults = []
        self._currentRowDescription = []

        self.send_StartupMessage()

    def changeStatus(self, newStatus):
        self.transactionStatus = newStatus
        if newStatus == 'I':
            query = self._queries.get()
            query.addCallback(self.send_Query)

    def runQuery(self, query):
        d = Deferred()
        self._dfds.append(d)
        self._queries.put(query)
        return d

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

    def send_Query(self, query):
        if not query.endswith('\x00'):
            query = query + '\x00'
        self.send('Q', query)

    @handler
    def handle_Authentication(self, data):
        if len(data) == 8:
            print 'md5 auth required'
        elif len(data) == 4:
            print 'r4'

    @handler
    def handle_Unknown(self, data):
        print 'unknown handler, data: %r' % (data, )

    @handler
    def handle_ParameterStatus(self, data):
        param, value, empty = data.split('\x00')
        self.parameters[param] = value

    @handler
    def handle_BackendKeyData(self, data):
        self.backendPID, self.cancellationKey = struct.unpack('!II', data)

    @handler
    def handle_ReadyForQuery(self, data):
        self.changeStatus(data)

    @handler
    def handle_ErrorResponse(self, data):
        fields = data.split('\x00')
        for field in fields:
            if not field:
                continue
            fieldType = field[0]
            fieldValue = field[1:]
            print 'SERVER ERROR! %s: %s' % (fieldType, fieldValue)

    @handler
    def handle_RowDescription(self, data):
        numFields = struct.unpack('!h', data[:2])
        data = data[2:]

        while data:
            fieldname, null, data = data.partition('\x00')
            table_oid, attrnum, type_oid, typlen, typmod, formatCode = struct.unpack('!IhIhIh', data[:18])
            data = data[18:]
            self._currentRowDescription.append(fieldname)

    @handler
    def handle_DataRow(self, data):
        numCols = struct.unpack('!h', data[:2])
        data = data[2:]
        row = []
        while data:
            fieldLen = struct.unpack('!I', data[:4])[0]
            fieldData = data[4:4 + fieldLen]
            data = data[4 + fieldLen:]
            row.append(fieldData)
        self._currentResults.append(row)

    @handler
    def handle_CommandComplete(self, data):
        dfd = self._dfds.popleft()
        dfd.callback(self._currentResults)
        self._currentRowDescription = []
        self._currentResults = []