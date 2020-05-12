from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import datetime
import config, csv



ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3')

csvFilePath = 'freeway_stations.csv'
data = []

with open(csvFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        data.append(rows)

for d in data:
    for key in list(d.keys()):
        if ( (key == "stationclass") or (key == "numberlanes") or (key == "lation") ):
            del d[key]

    session.execute('INSERT INTO freeway_stations JSON \' ' + json.dumps(d) + '\'');

cluster.shutdown()
