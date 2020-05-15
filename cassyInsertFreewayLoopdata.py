from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import datetime
import config, csv, json



ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_2_testing_12')

csvFilePath = 'freeway_loopdata_OneHour.csv'
data = []

with open(csvFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        data.append(rows)

for d in data:
    for key in list(d.keys()):
        if ( (key == "status") or (key == "dqflags") or (d[key] == "") ):
            del d[key]

    stObject = datetime.strptime(d['starttime'], '%m/%d/%Y %H:%M:%S')
    d['starttime'] = datetime.strftime(stObject, '%Y-%m-%d %H:%M:%S')
    session.execute('INSERT INTO loopdata_by_detector JSON \' ' + json.dumps(d) + '\'');



cluster.shutdown()
