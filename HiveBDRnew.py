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

def lookupTables(databasename):

# For testing, until firewall opened, pull tablenames from target cluster. We are therefore simulating an overwrite.
# For real, change impalahost to pull tablenames from source cluster.
#   impala_host = 'br1anprhsn02.passporthealth.com'

    impala_host = 'br1andvhsn01.passporthealth.com'

    show_statement = "'show tables in " + databasename +"'"
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
    return tablenames

def main(passed_username, passed_password, passed_database, passed_tables):

    if len(passed_tables) == 0:
        tablenames = lookupTables(passed_database)

    exit(0)

    PEER_NAME = 'PRODUCTION'                        # Previously
    TARGET_CLUSTER_NAME = 'DEV'                     #     defined
    SOURCE_CLUSTER_NAME = 'cluster'                 #       at Experian

    cm_host = 'br1andvhmn11.passporthealth.com'


    cm_client.configuration.username = passed_username

    cm_client.configuration.password = passed_password

    sourceDatabase = passed_database

    # Setup authentication for SSL
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



    tablenames = []
    tablenames.append('b05006')
    tablenames.append('b05007_bgpr_cape_rename')
    tablenames.append('b07001_bgpr_cape')
    tablenames.append('b13008_bgpr_cape')
    tablenames.append('c16001_bgpr_cape')
    tablenames.append('c24070_bgpr_cape')
    tablenames.append('cde2018_bg_a1')
    tablenames.append('cde2018_bg_a2')
    tablenames.append('cde2018_bg_a3')
    tablenames.append('cde2018_bg_a4')
    tablenames.append('cde2018_bg_b')
    tablenames.append('cde2018_bg_c1')
    tablenames.append('cde2018_bg_c2')
    tablenames.append('cde2018_bg_c3')
    tablenames.append('cde2018_bg_d')
    tablenames.append('cons_exp_annual_cye2018_bg')
    tablenames.append('cons_exp_annual_fyp2018_bg')
    tablenames.append('cons_exp_avgannual_cye2018_bg')
    tablenames.append('cons_exp_avgannual_fyp2018_bg')
    tablenames.append('cons_exp_gifts_cye2018_bg')
    tablenames.append('cons_exp_gifts_fyp2018_bg')
    tablenames.append('cye2018_bg_a1')
    tablenames.append('cye2018_bg_a2')
    tablenames.append('cye2018_bg_a3')
    tablenames.append('cye2018_bg_a4')
    tablenames.append('cye2018_bg_b')
    tablenames.append('cye2018_bg_c1')
    tablenames.append('cye2018_bg_c2')
    tablenames.append('cye2018_bg_c3')
    tablenames.append('cye2018_bg_d')
    tablenames.append('fyp2018_bg_a1')
    tablenames.append('fyp2018_bg_a2')
    tablenames.append('fyp2018_bg_a3')
    tablenames.append('fyp2018_bg_a4')
    tablenames.append('fyp2018_bg_b')
    tablenames.append('fyp2018_bg_c1')
    tablenames.append('fyp2018_bg_c2')
    tablenames.append('fyp2018_bg_c3')
    tablenames.append('fyp2018_bg_d')

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
#       table.database = (passed_database)
#
#  Hardwire database name for 2020-11-24 replication
#
        table.database = 'lake_consumerview'
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
#        hdfs_args.destinationPath = '/'
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
    parser.add_argument("username", help="user to run the BDR jobs")
    parser.add_argument("password", help="password of the user")
    parser.add_argument("database", help="database to replicate from source to target cluster using BDR")
    parser.add_argument("tables", nargs='*',help="optional: specific subset of tables to replicate")
    args = parser.parse_args()

    main(args.username, args.password, args.database, args.tables)
