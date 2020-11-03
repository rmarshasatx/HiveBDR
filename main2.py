import cm_client
from cm_client.rest import ApiException
from pprint import pprint

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("username")
parser.add_argument("password")
args = parser.parse_args()

# Configure HTTP basic authorization: basic
cm_client.configuration.username = args.username
cm_client.configuration.password = args.password

# print(cm_client.configuration.username, cm_client.configuration.password)


# Look for CDH6 clusters
if cluster.full_version.startswith("6."):
    services_api_instance = cm_client.ServicesResourceApi(api_client)
    services = services_api_instance.read_services(cluster.name, view='FULL')
    for service in services.items:
        print service.display_name, "-", service.type
        if service.type == 'HDFS':
            hdfs = service
