from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import config, csv, json
from datetime import datetime



ap = PlainTextAuthProvider(username=config.username, password=config.password)
node_ips = config.hosts
cluster = Cluster(node_ips, protocol_version=4, auth_provider=ap, port=config.port)
session = cluster.connect('part_2_testing')

# https://medium.com/@hannah15198/convert-csv-to-json-with-python-b8899c722f6d
csvFilePath = 'freeway_loopdata_OneHour.csv'
jsonFilePath = 'freeway_loopdata_OneHour.json'
data = []

with open(csvFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for rows in csvReader:
        data.append(rows)

#with open(jsonFilePath, 'w') as jsonFile:
for d in data:
    stObject = datetime.strptime(d['starttime'], '%m/%d/%Y %H:%M:%S')
    d['starttime'] = datetime.strftime(stObject, '%Y-%m-%d %H:%M:%S')
    session.execute('INSERT INTO freeway_loopdata_OneHour JSON \' ' + json.dumps(d) + '\'');

    #jsonFile.write(json.dumps(data, indent = 0))

cluster.shutdown()

