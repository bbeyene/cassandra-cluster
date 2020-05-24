from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import config

""" Find the number of speeds > 100 in the data set. """

ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3_version_0')

query = 'SELECT speed FROM loopdata_by_detector'
statement = SimpleStatement(query, fetch_size=5000)

count = 0
for row in session.execute(statement):
    if isinstance(row.speed, int) and row.speed > 100:
        count+=1

print("\nNumber of speeds > 100: " + str(count) + "\n")
cluster.shutdown()

