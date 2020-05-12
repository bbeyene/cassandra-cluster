from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import config
from datetime import datetime
from cassandra.query import dict_factory 



ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)

session = cluster.connect('part_2_testing_2')
session.row_factory = dict_factory 
rows = session.execute( 'select * from freeway_loopdata_OneHour where detectorid = 1346 and starttime >= \'2011-09-15\' and starttime < \'2011-09-15 00:10:00\' ');

results = []

for row in rows:
    results.append(row)


for r in results:
    print(r['detectorid'], r['starttime'])


cluster.shutdown()

