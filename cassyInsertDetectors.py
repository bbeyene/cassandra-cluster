from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import config, csv


ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3')


highwaysFilePath = 'highways.csv'
stationsFilePath = 'freeway_stations.csv'
detectorsFilePath = 'freeway_detectors.csv'

highways = {} 
stations = {} 
detectors = []

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
        detectors.append(rows)

for d in detectors:
    did = d['detectorid']
    sid = d['stationid']
    hid = d['highwayid']
    direction = highways[hid]['direction']
    highwayname = highways[hid]['highwayname']
    locationtext = stations[sid]['locationtext']
    length = stations[sid]['length']
    upstream = stations[sid]['upstream']
    downstream = stations[sid]['downstream']
    currentStr = "\'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}".format(direction, highwayname, locationtext, did, length, upstream, downstream, sid, hid)
    columnStr = "direction, highwayname, locationtext, detectorid, length, upstream, downstream, stationid, highwayid"
    query = "INSERT INTO detectors_by_highway ( " + columnStr + " ) " + " VALUES ( " + currentStr + " ) "

    session.execute(query)

cluster.shutdown()
