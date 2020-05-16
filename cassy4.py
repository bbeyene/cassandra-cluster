from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import datetime
import config, csv, json


ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_2_testing_13')


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

for key, value in detectors.items():
    did = str(key)
    sid = str(value['stationid'])
    hid = str(value['highwayid'])
    direction = str(highways[hid]['direction'])
    highwayname = str(highways[hid]['highwayname'])
    locationtext = str(stations[sid]['locationtext'])
    length = str(stations[sid]['length'])
    upstream = str(stations[sid]['upstream'])
    downstream = str(stations[sid]['downstream'])
    currentStr = "\'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}".format(direction, highwayname, locationtext, did, length, upstream, downstream, sid, hid)
    columnStr = "direction, highwayname, locationtext, detectorid, length, upstream, downstream, stationid, highwayid"
    query = "INSERT INTO detectors_by_highway ( " + columnStr + " ) " + " VALUES ( " + currentStr + " ) "

    print(query)
"""
        session.execute(query)

with open(loopdataFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        loopdataByDetector.append(rows)

for l in loopdataByDetector:
    for key in list(l.keys()):
        del d['occupancy']
        del d['dqflags']
        del d['status']
        if (l[key] == ""):
            del l[key]
    stObject = datetime.strptime(l['starttime'], '%m/%d/%Y %H:%M:%S')
    l['starttime'] = datetime.strftime(stObject, '%Y-%m-%d %H:%M:%S')
    # OR.... just insert speed, volume, starttime and detectorid directly
    session.execute('INSERT INTO loopdata_by_detector JSON \' ' + json.dumps(l) + '\'')
"""
cluster.shutdown()
