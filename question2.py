from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import config

""" Find the total volume for the station Foster NB for Sept 21, 2011. """

ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3_version_0')

location = 'Foster NB'
detectorids = []
results = session.execute("SELECT detectorid FROM detectors_by_highway WHERE locationtext = %s", ["Foster NB"])
for row in results:
    detectorids.append(row.detectorid)

temp = ''
for d in detectorids:
    temp += str(d)
    temp += ', '

idList = temp[0:-2]

query = "SELECT SUM(volume) FROM loopdata_by_detector WHERE detectorid IN ( " + idList + " ) AND starttime >= \'2011-09-21\' AND starttime < \'2011-09-23\' "
result = session.execute(query)

for row in result:
    print(row.system_sum_volume)

cluster.shutdown()

