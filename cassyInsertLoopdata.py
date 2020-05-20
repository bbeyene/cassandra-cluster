from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from datetime import datetime
import config, csv, json


ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_3_testing_4')


loopdataFilePath = 'freeway_loopdata_OneHour.csv'

loopdataByDetector = []

with open(loopdataFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)

    insertNone = "INSERT INTO loopdata_by_detector (detectorid, starttime) VALUES (?, ?)"
    insertVolume = "INSERT INTO loopdata_by_detector (detectorid, starttime, volume) VALUES (?, ?, ?)"
    insertSpeed = "INSERT INTO loopdata_by_detector (detectorid, starttime, speed) VALUES (?, ?, ?)"
    insertBoth = "INSERT INTO loopdata_by_detector (detectorid, starttime, speed, volume) VALUES (?, ?, ?, ?)"
    preparedNone = session.prepare(insertNone)
    preparedVolume = session.prepare(insertVolume)
    preparedSpeed = session.prepare(insertSpeed)
    preparedBoth = session.prepare(insertBoth)

    for rows in csvReader:
        dtObject = datetime.strptime(rows['starttime'], '%m/%d/%Y %H:%M:%S')
        dtString = datetime.strftime(dtObject, '%Y-%m-%d %H:%M:%S')
        if rows['speed'] == '0' or rows['speed'] == '':
            if rows['volume'] == '':
                session.execute(preparedNone, (int(rows['detectorid']), dtObject))
            else: 
                session.execute(preparedVolume, (int(rows['detectorid']), dtObject, int(rows['volume'])))
        elif rows['volume'] == '':
            session.execute(preparedSpeed, (int(rows['detectorid']), dtObject, int(rows['speed'])))
        else:
            session.execute(preparedBoth, (int(rows['detectorid']), dtObject, int(rows['speed']), int(rows['volume'])))

cluster.shutdown()
