from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from datetime import datetime
import config, csv, json


ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3_version_0')

loopdataFilePath = 'freeway_loopdata.csv'

with open(loopdataFilePath) as csvFile:
    csvReader = csv.reader(csvFile)
    header = next(csvReader)

    preparedNone = session.prepare("INSERT INTO loopdata_by_detector (detectorid, starttime) VALUES (?, ?)")
    preparedVolume = session.prepare("INSERT INTO loopdata_by_detector (detectorid, starttime, volume) VALUES (?, ?, ?)")
    preparedSpeed = session.prepare("INSERT INTO loopdata_by_detector (detectorid, starttime, speed) VALUES (?, ?, ?)")
    preparedBoth = session.prepare("INSERT INTO loopdata_by_detector (detectorid, starttime, speed, volume) VALUES (?, ?, ?, ?)")

    for rows in csvReader:
        dtObject = datetime.strptime(rows[1][0:19], '%Y-%m-%d %H:%M:%S')

        if rows[3] == '0' or rows[3] == '':
            if rows[2] == '':
                session.execute(preparedNone, (int(rows[0]), dtObject))
            else: 
                session.execute(preparedVolume, (int(rows[0]), dtObject, int(rows[2])))
        elif rows[2] == '':
            session.execute(preparedSpeed, (int(rows[0]), dtObject, int(rows[3])))
        else:
            session.execute(preparedBoth, (int(rows[0]), dtObject, int(rows[3]), int(rows[2])))

cluster.shutdown()
