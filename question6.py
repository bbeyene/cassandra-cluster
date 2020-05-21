from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import config


""" Find a route from Johnson Creek to Columbia Blvd on I-205 NB using the upstream and downstream fields. """

ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)

session = cluster.connect('part_3_version_0')
preparedInit = session.prepare( 'SELECT locationtext, upstream, downstream FROM detectors_by_highway WHERE direction = ? AND highwayname = ? AND locationtext = ? limit 1')
preparedUp = session.prepare( 'SELECT locationtext, upstream, downstream FROM detectors_by_highway WHERE direction = ? AND highwayname = ? AND upstream = ? limit 1')
preparedDown = session.prepare( 'SELECT locationtext, upstream, downstream FROM detectors_by_highway WHERE direction = ? AND highwayname = ? AND downstream = ? limit 1')

start = 'Johnson Cr NB'
end = 'Columbia to I-205 NB'

curr = session.execute(preparedInit, ('NORTH', 'I-205', start))
up = session.execute(preparedInit, ('NORTH', 'I-205', start))
down = session.execute(preparedInit, ('NORTH', 'I-205', start))

while (curr[0].locationtext != end):
    #print(curr[0].locationtext)
    curr = session.execute(preparedUp, ('NORTH', 'I-205', x))

    
print(rows[0].locationtext)

cluster.shutdown()

