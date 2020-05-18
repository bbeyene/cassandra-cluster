from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import datetime
import config, csv, json


ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3_testing_3')


loopdataFilePath = 'freeway_loopdata_oneday.csv'

loopdataByDetector = []

with open(loopdataFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        loopdataByDetector.append(rows)

for l in loopdataByDetector:
    l.pop('status')
    l.pop('dqflags')
    l.pop('occupancy')
    if l['speed'] == '0' or l['speed'] == '':
        l.pop('speed')
    if l['volume'] == '':
        l.pop('volume')

    stObject = datetime.strptime(l['starttime'], '%m/%d/%Y %H:%M:%S')
    l['starttime'] = datetime.strftime(stObject, '%Y-%m-%d %H:%M:%S')
    # OR.... just insert speed, volume, starttime and detectorid directly
    session.execute('INSERT INTO loopdata_by_detector JSON \' ' + json.dumps(l) + '\'')

cluster.shutdown()
