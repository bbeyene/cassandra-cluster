from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import config

ap = PlainTextAuthProvider(username=config.username, password=config.password)

node_ips = ['127.0.0.1']

#cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=9042)
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=9042)

session = cluster.connect('part_1')
rows = session.execute('select * from loopdata_one_hour');
cluster.shutdown()

for row in rows:
      print(row)
