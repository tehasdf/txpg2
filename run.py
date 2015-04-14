from txpg2 import PostgresProtocol

from twisted.internet import reactor
from twisted.internet.protocol import ClientFactory

reactor.connectUNIX('/var/run/postgresql/.s.PGSQL.5432', ClientFactory.forProtocol(PostgresProtocol), timeout=1)
reactor.run()