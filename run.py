from txpg2 import PostgresProtocol

from twisted.internet import reactor
from twisted.internet.protocol import ClientFactory
from twisted.internet.endpoints import UNIXClientEndpoint
from twisted.internet.task import react
from twisted.internet.defer import inlineCallbacks

@inlineCallbacks
def main(reactor):
    ep = UNIXClientEndpoint(reactor, '/var/run/postgresql/.s.PGSQL.5432')
    fact = ClientFactory.forProtocol(PostgresProtocol)
    proto = yield ep.connect(fact)
    # for i in range(5000):
    res = yield proto.runQuery('SELECT * from entries limit 5')


react(main)