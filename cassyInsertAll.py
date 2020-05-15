from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import datetime
import config, csv, json


ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_2_testing_3')


highwaysFilePath = 'highways.csv'
stationsFilePath = 'freeway_stations.csv'
detectorsFilePath = 'freeway_detectors.csv'
loopdataFilePath = 'freeway_loopdata_OneHour.csv'

highways = []
stations = []
detectors = []
loopdataByDetector = []

with open(highwaysFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        highways.append(rows)

for h in highways:
    for key in list(s.keys()):
        if ( (key == "shortdirection")
            del h[key]

with open(stationsFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        stations.append(rows)

for s in stations:
    for key in list(s.keys()):
        if ( (key == "stationclass") or (key == "numberlanes") or (key == "lation") ):
            del s[key]

with open(detectorsFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        detectors.append(rows)

for d in detectors:
    for key in list(d.keys()):
        if ( (key == "locationtext") or (key == "detectorclass") or (key == "lanenumber") ):
            del d[key]


#pre-joins






with open(loopdataFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        loopdataByDetector.append(rows)

for l in loopdataByDetector:
    for key in list(l.keys()):
        if ( (key == "status") or (key == "dqflags") or (l[key] == "") ):
            del d[key]
    stObject = datetime.strptime(d['starttime'], '%m/%d/%Y %H:%M:%S')
    d['starttime'] = datetime.strftime(stObject, '%Y-%m-%d %H:%M:%S')
    session.execute('INSERT INTO loopdata_by_detector JSON \' ' + json.dumps(d) + '\'');

for d in detectorsByLoacationtext:
    session.execute('INSERT INTO detectors_by_locationtext JSON \' ' + json.dumps(d) + '\'');

cluster.shutdown()
