#!/usr/bin/env python

import cm_client
from cm_client.rest import ApiException

from cm_api.api_client import ApiResource
from cm_api.endpoints.types import *

cm_host = 'br1andvhmn11.passporthealth.com'

from pprint import pprint
import argparse
import os

PEER_NAME = 'PRODUCTION'
TARGET_CLUSTER_NAME = 'DEV'
SOURCE_CLUSTER_NAME = 'cluster'


parser = argparse.ArgumentParser()
parser.add_argument("username")
parser.add_argument("password")
parser.add_argument("database")
args = parser.parse_args()

# Setup authentication for SSL
cm_client.configuration.username = args.username
cm_client.configuration.password = args.password
sourceDatabase = args.database

cm_client.configuration.verify_ssl = True
cm_client.configuration.ssl_ca_cert = '/opt/cloudera/security/pki/x509/truststore.pem'

# Create an instance of the API class
api_host = 'https://br1andvhmn11.passporthealth.com'
port = '7183'
api_version = 'v30'

impala_host = 'br1andvhsn01.passporthealth.com'

# Construct base URL for API
# http://cmhost:7180/api/v30
api_url = api_host + ':' + port + '/api/' + api_version
api_client = cm_client.ApiClient(api_url)
cluster_api_instance = cm_client.ClustersResourceApi(api_client)

# Lists all known clusters.
api_response = cluster_api_instance.read_clusters(view='SUMMARY')
for cluster in api_response.items:
    print cluster.name, "-", cluster.full_version

    services_api_instance = cm_client.ServicesResourceApi(api_client)
    services = services_api_instance.read_services(cluster.name, view='FULL')
    for service in services.items:
#        print service.display_name, "-", service.type
        if service.type == 'HIVE':
		targetHive = service
		targetCluster = cluster

		print targetHive.name, targetHive.service_state, targetHive.health_summary
		
		for health_check in targetHive.health_checks:
			print health_check.name, "---", health_check.summary

#		print "Source database = " + sourceDatabase

show_statement = "'show tables in " + sourceDatabase +"'"
streamOperand = "impala-shell -i " + impala_host + " -d default -k --ssl --ca_cert=/opt/cloudera/security/pki/x509/truststore.pem -q " + show_statement
stream=os.popen(streamOperand)

output=stream.readlines()
lineno =0
numtables = 0
tablenames = []
for line in output:
    if lineno <= 2:     # skip heading lines
        pass
    elif line[0:3] == "+--":                    # skip last line
        pass
    else:                                       # strip out tablename
        name = line[2:]
        blank = name.index(' ')
        tablenames.append(name[0:blank])
        numtables +=1
    lineno +=1
print str(numtables) + " tables in database " + sourceDatabase
for table in tablenames:
	print table

api_root = ApiResource(cm_host, username=args.username, password=args.password,  use_tls=True)

PEER_NAME = 'PRODUCTION'
SOURCE_HDFS_NAME = 'hdfs'
TARGET_HDFS_NAME = 'hdfs'
SOURCE_HIVE_NAME = 'hive'
TARGET_HIVE_NAME = 'hive'
SOURCE_CLUSTER_NAME = 'cluster'
TARGET_CLUSTER_NAME = 'DEV'
TARGET_YARN_SERVICE = 'yarn'

hdfs = api_root.get_cluster(TARGET_CLUSTER_NAME).get_service(TARGET_HDFS_NAME)
hdfs_args = ApiHdfsReplicationArguments(None)
hdfs_args.sourceService = ApiServiceRef(None, peerName=PEER_NAME, clusterName=SOURCE_CLUSTER_NAME, serviceName=SOURCE_HDFS_NAME)
hdfs_args.sourcePath = '/user/bob.marshall/repltest'
hdfs_args.destinationPath = '/user/bob.marshall/rep/test'
hdfs_args.mapreduceServiceName = TARGET_YARN_SERVICE
hdfs_args.userName = args.username
hdfs_args.sourceUser = args.username
hdfs_args.preserveBlockSize = True
hdfs_args.preserveReplicationCount = True
hdfs_args.preservePermissions = True
hdfs_args.skipChecksumChecks = True
hdfs_args.skipListingChecksumChecks = True
start = datetime.datetime.now()
end = start + datetime.timedelta(days=1)
interval = "DAY"
numinterval = 1
pause = True
#schedule = hdfs.create_replication_schedule(start, end, interval, interval, pause, hdfs_args)
print "Creating HDFS Replication Schedule"
schedule = hdfs.create_replication_schedule(start, end, "DAY", 1, True, hdfs_args)
print "Starting HDFS Replication"
cmd = hdfs.trigger_replication_schedule(schedule.id)
print "Waiting for completion"
cmd = cmd.wait()
print "Getting result"
result = hdfs.get_replication_schedule(schedule.id).history[0].hdfsResult
print result

print "Cleanup... Remove HDFS replication schedule"
sch = hdfs.delete_replication_schedule(schedule.id)
print sch

#scheds = hdfs.get_replication_schedules()
#sch = hdfs.delete_replication_schedule(27)




#hive =  api_root.get_cluster(TARGET_CLUSTER_NAME).get_service(TARGET_HIVE_NAME)
#scheds = hive.get_replication_schedules()
#sch = hive.delete_replication_schedule(162)

