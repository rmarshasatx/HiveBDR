#!/usr/bin/env python
"""
Script to replicate all the tables in a DB. Written to gret around issues/limits with replicating an entire DB with Cloudera Hive BDR jobs.

First major refactoring - Create main program structure.
"""

__author__ = "phdata, Inc"
__version = "0.3.0"
__license__ = "ASFv2"

import cm_client                                # API v.30 and up
from cm_client.rest import ApiException

from cm_api.api_client import ApiResource       # Deprecated from API v.30 onwards
from cm_api.endpoints.types import *            # but still supported in CM v. 6




import argparse
import os

def main(passed_username, passed_password, passed_database):

    PEER_NAME = 'PRODUCTION'                        # Previously
    TARGET_CLUSTER_NAME = 'DEV'                     #     defined
    SOURCE_CLUSTER_NAME = 'cluster'                 #       at Experian

    cm_host = 'br1andvhmn11.passporthealth.com'


    cm_client.configuration.username = passed_username

    # Ensure that password is quoted
    cm_client.configuration.password = "'" + passed_password + "'"

    sourceDatabase = passed_database

    # Setup authentication for SSL
    cm_client.configuration.verify_ssl = True
    cm_client.configuration.ssl_ca_cert = '/opt/cloudera/security/pki/x509/truststore.pem'

    # Create an instance of the API class
    api_host = 'https://br1andvhmn11.passporthealth.com'
    port = '7183'
    api_version = 'v30'

    impala_host = 'br1anprhsn02.passporthealth.com'

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

    ###show_statement = "'show tables in " + sourceDatabase +"'"
    ###streamOperand = "impala-shell -i " + impala_host + " -d default -k --ssl --ca_cert=/opt/cloudera/security/pki/x509/truststore.pem -q " + show_statement
    ###stream=os.popen(streamOperand)
    ###
    ###output=stream.readlines()
    ###lineno =0
    ###numtables = 0
    ###tablenames = []
    ###for line in output:
    ###    if lineno <= 2:     # skip heading lines
    ###        pass
    ###    elif line[0:3] == "+--":                    # skip last line
    ###        pass
    ###    else:                                       # strip out tablename
    ###        name = line[2:]
    ###        blank = name.index(' ')
    ###        tablenames.append(name[0:blank])
    ###        numtables +=1
    ###    lineno +=1
    ###print str(numtables) + " tables in database " + sourceDatabase
    ###for table in tablenames:
    ###	print table


    tablenames = []
    tablenames.append("test")
    tablenames.append("test2")


    api_root = ApiResource(cm_host, username=passed_username, password=passed_password,  use_tls=True)

    PEER_NAME = 'PRODUCTION'
    SOURCE_HDFS_NAME = 'hdfs'
    TARGET_HDFS_NAME = 'hdfs'
    SOURCE_HIVE_NAME = 'hive'
    TARGET_HIVE_NAME = 'hive'
    SOURCE_CLUSTER_NAME = 'cluster'
    TARGET_CLUSTER_NAME = 'DEV'
    TARGET_YARN_SERVICE = 'yarn'




    # Setup for Hive replication
    hive =  api_root.get_cluster(TARGET_CLUSTER_NAME).get_service(TARGET_HIVE_NAME)
    hive_args = ApiHiveReplicationArguments(None)
    hdfs_args = ApiHdfsReplicationArguments(None)                   # Needed for replicating table data stored in HDFS
    hive_args.sourceService = ApiServiceRef(None, peerName=PEER_NAME, clusterName=SOURCE_CLUSTER_NAME, serviceName=SOURCE_HIVE_NAME)

    # Define tables to replicate
    table_filters = []
    table = ApiHiveTable(None)

    for tab in tablenames:
        table.database = (passed_database)
        table.tableName = (tab)
        table_filters = []
        table_filters.append(table)
        print "Replicating " + passed_database + "." + tab

        hive_args.tableFilters = table_filters
        hive_args.force = True                                          # Overwrite existing tables
        hive_args.replicateData = True                                  # Replicate table data stored in HDFS
        hdfs_args.skipChecksumChecks = True
        hdfs_args.skipListingChecksumChecks = True
        hdfs_args.preserveBlockSize = True
        hdfs_args.preserveReplicationCount = True
        hdfs_args.preservePermissions = True

        # Define HDFS portion of the Hive replication as needed
        hdfs_args.destinationPath = '/user/bob.marshall/repltest'       # Argument? Path relative to servicename?
        hdfs_args.mapreduceServiceName = TARGET_YARN_SERVICE
        hdfs_args.userName = passed_username
        hdfs_args.sourceUser = passed_username

        hive_args.hdfsArguments = hdfs_args

        start = datetime.datetime.now()
        end = start + datetime.timedelta(days=1)
        interval = "DAY"
        numinterval = 1
        pause = True

        print "Creating Hive Replication Schedule"
        schedule = hive.create_replication_schedule(start, end, interval, numinterval, pause, hive_args)
        print "Starting Hive Replication"
        cmd = hive.trigger_replication_schedule(schedule.id)
        print "Waiting for completion"
        cmd = cmd.wait()
        print "Getting result"
        result = hive.get_replication_schedule(schedule.id).history[0].hiveResult
        print result

        print "Cleanup... Remove Hive replication schedule"
        sch = hive.delete_replication_schedule(schedule.id)
        print sch

    exit(0)

#scheds = hive.get_replication_schedules()
#sch = hive.delete_replication_schedule(162)


# Setup for HDFS replication
hdfs = api_root.get_cluster(TARGET_CLUSTER_NAME).get_service(TARGET_HDFS_NAME)
hdfs_args = ApiHdfsReplicationArguments(None)
hdfs_args.sourceService = ApiServiceRef(None, peerName=PEER_NAME, clusterName=SOURCE_CLUSTER_NAME, serviceName=SOURCE_HDFS_NAME)
hdfs_args.sourcePath = '/user/bob.marshall/repltest'
hdfs_args.destinationPath = '/user/bob.marshall/repltest'
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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Perform BDR jobs while getting around BDR limitations.')
    parser.add_argument("username")
    parser.add_argument("password")
    parser.add_argument("database")
    args = parser.parse_args()

    main(args.username, args.password, args.database)