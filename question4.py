from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import config

""" Find the average travel time for 7-9AM and 4-6PM on Sept 22, 2011 for station Foster NB in seconds """

ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3_version_0')

results = session.execute(
                """
                SELECT detectorid, length
                FROM detectors_by_highway 
                WHERE locationtext = %s
                """, ["Foster NB"])

length = results[0].length
temp = ''
for row in results:
    temp += str(row.detectorid) + ', '
idList = temp[0:-2]

queryMorning = """
               SELECT AVG(speed) 
               FROM loopdata_by_detector 
               WHERE detectorid 
               IN ( """ + idList + """ ) 
               AND starttime >= \'2011-09-22 07:00:00\' 
               AND starttime < \'2011-09-22 09:00:00\' 
               """
queryEvening = """
               SELECT AVG(speed) 
               FROM loopdata_by_detector 
               WHERE detectorid 
               IN ( """ + idList + """ ) 
               AND starttime >= \'2011-09-22 16:00:00\' 
               AND starttime < \'2011-09-22 18:00:00\' 
               """

morningResult = session.execute(queryMorning)
eveningResult = session.execute(queryEvening)

morningAvg = morningResult[0][0]
eveningAvg = eveningResult[0][0]
speedAvg = (morningAvg + eveningAvg) / 2

travelTime = (float(length) / speedAvg) * 3600
print("\nAvg travel time for 7-9AM and 4-6pm on Sept 22, 2011 for Foster NB: " + str(round(travelTime)) + " seconds\n")

cluster.shutdown()

