from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import timedelta, datetime
import config

""" Find the travel time for station Foster NB for 5-minute intervals for Sept 22, 2011 """

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

intervals = []
first = datetime(2011, 9, 22, tzinfo=None)
increment = timedelta(minutes=5)
last = datetime(2011, 9, 23, tzinfo=None)

intervals.append(first)
inserted = first
while(inserted < last):
    insert = inserted + increment
    intervals.append(insert)
    inserted = insert

prepared = session.prepare(
            """ SELECT starttime, AVG(speed) as average
                FROM loopdata_by_detector 
                WHERE detectorid 
                IN ( """ + idList + """ ) 
                AND starttime >= ? 
                AND starttime < ?
            """ )

travelTimes = {}
intervalLength = len(intervals) - 1
i = 0
while i < intervalLength:
    row = session.execute(prepared, (intervals[i], intervals[i+1]))
    if isinstance(row[0].starttime, datetime):
        st = row[0].starttime
        st = st - timedelta(minutes=st.minute % 5, seconds=st.second)
        key = datetime.strftime(st, '%Y-%m-%d %H:%M:%S')
        value = row[0].average
        travelTimes[key] = round((float(length)/value) * 3600, 1)
    i += 1

travelTimesOrdered = sorted(travelTimes.items(), key=lambda x: x[0])
for tt in travelTimesOrdered:
    print(tt)

cluster.shutdown()
