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

highways = {} 
stations = {} 
detectors = {}
loopdataByDetector = []

with open(highwaysFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        highwayid = rows['highwayid']
        highways[highwayid] = rows

with open(stationsFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        stationid = rows['stationid']
        stations[stationid] = rows

with open(detectorsFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        detectorid = rows['detectorid']
        detectors[detectorid] = rows;

#this wont work - must do join as planned before
for key, value in detectors.items():
        print(key + ' ' +  value['locationtext'] + ' ' + stations[value['stationid']])

#print(stations['1045']['upstream'])
"""

session.execute('INSERT INTO detectors_by_highway (');

with open(loopdataFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        loopdataByDetector.append(rows)

for l in loopdataByDetector:
    for key in list(l.keys()):
        #if ( (key == "occupancy") or (key == "status") or (key == "dqflags") or (l[key] == "") ):
            del d[key]
        del d['occupancy']
        del d['dqflags']
        del d['status']
        if (l[key] == ""):
            del d[key]
    stObject = datetime.strptime(d['starttime'], '%m/%d/%Y %H:%M:%S')
    d['starttime'] = datetime.strftime(stObject, '%Y-%m-%d %H:%M:%S')
    # OR.... just insert speed, volume, starttime and detectorid directly duh
    session.execute('INSERT INTO loopdata_by_detector JSON \' ' + json.dumps(d) + '\'');

for d in detectorsByLoacationtext:
    session.execute('INSERT INTO detectors_by_locationtext JSON \' ' + json.dumps(d) + '\'');

cluster.shutdown()
"""
