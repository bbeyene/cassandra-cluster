from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import datetime
import config, csv



ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3')

csvFilePath = 'freeway_detectors.csv'
data = []

with open(csvFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        data.append(rows['detectorid'], rows['stationid'], rows['milepost'], rows['highwayid'])

for d in data:
    for key in list(d.keys()):
        if ( (key == "locationtext") or (key == "detectorclass") or (key == "lanenumber") ):
            del d[key]

    session.execute('INSERT INTO freeway_detectors JSON \' ' + json.dumps(d) + '\'');

cluster.shutdown()
