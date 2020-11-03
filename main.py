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


