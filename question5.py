from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import config

""" Find the average travel time for 7-9AM and 4-6PM on Sept 22, 2011 for I-205 NB freeway in minutes """

ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3_version_0')

session.row_factory = dict_factory

results = session.execute(
                """
                SELECT detectorid, locationtext, length
                FROM detectors_by_highway 
                WHERE direction = %s
                AND highwayname = %s
                """, ["NORTH", "I-205"])

lengths = {}
locations = {}
for r in results:
    lengths[r['detectorid']] = r['length']
    locations[r['detectorid']] = r['locationtext']

temp = ''
for key in lengths:
    temp += str(key) + ', '
idList = temp[0:-2]

queryMorning = """
               SELECT detectorid, AVG(speed) AS average
               FROM loopdata_by_detector 
               WHERE detectorid 
               IN ( """ + idList + """ ) 
               AND starttime >= \'2011-09-22 07:00:00\' 
               AND starttime < \'2011-09-22 09:00:00\' 
               GROUP BY detectorid
               """
queryEvening = """
               SELECT detectorid, AVG(speed) AS average
               FROM loopdata_by_detector 
               WHERE detectorid 
               IN ( """ + idList + """ ) 
               AND starttime >= \'2011-09-22 16:00:00\' 
               AND starttime < \'2011-09-22 18:00:00\' 
               GROUP BY detectorid
               """

morningResults = session.execute(queryMorning)
eveningResults = session.execute(queryEvening)

morningAverages = {}
for mr in morningResults:
    morningAverages[mr['detectorid']] = mr['average']
eveningAverages = {}
for er in eveningResults:
    eveningAverages[er['detectorid']] = er['average']

""" for each triplet: (length / ((morning avg + evening avg) / 2)) * 60 """

travelTimes = {}
for key, value in morningAverages.items():
    tempAvg = (value + eveningAverages[key]) / 2
    if tempAvg:
        travelTimes[key] = float(lengths[key]) / tempAvg
    else:
        travelTimes[key] = 0

stationTravelTimes = {}
for key, value in locations.items():
    if value in stationTravelTimes:
        stationTravelTimes[value] += travelTimes[key]
    else:
        stationTravelTimes[value] = travelTimes[key]

total = 0;
for key, value in stationTravelTimes.items():
    total += value

print("\nAvg travel time for 7-9AM and 4-6PM on Sept 22, 2011 for I-205 NB: " + str(round(total * 60, 1)) + " minutes\n")

cluster.shutdown()
