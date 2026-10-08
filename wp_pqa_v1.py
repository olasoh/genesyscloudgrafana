import os
import time
import sys
import datetime
import time

#print( time.mktime(datetime.datetime.now().timetuple()) )

from pprint import pprint
from dotenv import load_dotenv
from influxdb_client_3 import (
  InfluxDBClient3, InfluxDBError, Point, WritePrecision,
  WriteOptions, write_client_options)

import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException

# Load variables from .env file
load_dotenv()

# Access environment variables
host = os.getenv('INFLUX_HOST')
token = os.getenv('INFLUX_TOKEN')
database = os.getenv('INFLUX_DATABASE')

# Credentials
CLIENT_ID = os.environ['GENESYS_CLOUD_CLIENT_ID']
CLIENT_SECRET = os.environ['GENESYS_CLOUD_CLIENT_SECRET']
ORG_REGION = os.environ['GENESYS_CLOUD_REGION']  # eg. us_east_1

#print(f"Host: {host}")
#print(f"Token: {token}")
#print(f"Database: {database}")
#print(f"Client: {CLIENT_ID}")
#print(f"Secret: {CLIENT_SECRET}")
#print(f"Region: {ORG_REGION}")


# Set environment
region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
PureCloudPlatformClientV2.configuration.host = region.get_api_host()

# OAuth when using Client Credentials
api_client = PureCloudPlatformClientV2.api_client.ApiClient() \
            .get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)

# create an instance of the API class
api_instance = PureCloudPlatformClientV2.RoutingApi(api_client);
body = PureCloudPlatformClientV2.QueueObservationQuery() # QueueObservationQuery | query

page_number = 1 # int | Page number (optional) (default to 1)
page_size = 5 # int | Page size (optional) (default to 25)
sort_order = 'asc' # str | Note: results are sorted by name. (optional) (default to 'asc')
#name = 'PQR_AWIFP_PAY_CARE*' # str | Include only queues with the given name (leading and trailing asterisks allowed) (optional)
name = 'PQA_REA*' # str | Include only queues with the given name (leading and trailing asterisks allowed) (optional)



qualifiers = {}

def set_qualifier(qtmp, name: str):
    for status in qtmp:
        qualifiers[status] = name


# With batching mode, define callbacks to execute after a successful or
# failed write request.
# Callback methods receive the configuration and data sent in the request.
def success(self, data: str):
    #print(f"Successfully wrote batch: data: {data}")
    print(f"Successfully wrote batch")

def error(self, data: str, exception: InfluxDBError):
    print(f"Failed writing batch: config: {self}, data: {data} due: {exception}")

def retry(self, data: str, exception: InfluxDBError):
    print(f"Failed retry writing batch: config: {self}, data: {data} retry: {exception}")

# Configure options for batch writing.
write_options = WriteOptions(batch_size=1_000,
                                    flush_interval=10_000,
                                    jitter_interval=2_000,
                                    retry_interval=5_000,
                                    max_retries=5,
                                    max_retry_delay=30_000,
                                    exponential_base=2)

# Create an options dict that sets callbacks and WriteOptions.
wco = write_client_options(success_callback=success,
                          error_callback=error,
                          retry_callback=retry,
                          write_options=write_options)

# Instantiate a synchronous instance of the client with your
# InfluxDB credentials and write options, such as Gzip threshold, default tags,
# and timestamp precision. Default precision is nanosecond ('ns').

page_number = 1 # int | Page number (optional) (default to 1)
points = []


while True:

    try:
        # Get list of queues.
        #api_response = api_instance.get_routing_queues(page_number=page_number, page_size=page_size, sort_order=sort_order, name=name, id=id, division_id=division_id, peer_id=peer_id, canned_response_library_id=canned_response_library_id, has_peer=has_peer)
        api_response1 = api_instance.get_routing_queues(page_number=page_number, page_size=page_size, sort_order=sort_order, name=name)
        #pprint(api_response1)

        queues = {}
        predicates = []
        #pprint(body)


        for item in api_response1.entities:
            #print(f'{item.id},{item.name}')
            queues[item.id] = item.name
            predicates.append({
                        "type": "dimension",
                        "dimension": "queueId",
                        "operator": "matches",
                        "value": item.id
                    })

        body = {
            "filter": {
                "type": "and",
                "clauses": [
                {
                    "type": "or",
                    "predicates": predicates
                }
                ]
            },
            "metrics": [
                "oInteracting",
                "oWaiting",
                "oOnQueueUsers",
                "oOffQueueUsers"
            ]
        }

        #pprint(body)
        #sys.exit()

        try:
            # Query for queue observations
            api_response = api_instance.post_analytics_queues_observations_query(body)
            #pprint(api_response.system_to_organization_mappings)

            if api_response.system_to_organization_mappings is not None:

                #qualifiers = {}
                if api_response.system_to_organization_mappings.get('BUSY') is not None:
                    qtmp = api_response.system_to_organization_mappings.get('BUSY')
                    set_qualifier(qtmp, 'BUSY')
                if api_response.system_to_organization_mappings.get('ON_QUEUE') is not None:
                    qtmp = api_response.system_to_organization_mappings.get('ON_QUEUE')
                    set_qualifier(qtmp, 'ON_QUEUE')
                if api_response.system_to_organization_mappings.get('AVAILABLE') is not None:
                    qtmp = api_response.system_to_organization_mappings.get('AVAILABLE')
                    set_qualifier(qtmp, 'AVAILABLE')
                if api_response.system_to_organization_mappings.get('OFFLINE') is not None:
                    qtmp = api_response.system_to_organization_mappings.get('OFFLINE')
                    set_qualifier(qtmp, 'OFFLINE')
                if api_response.system_to_organization_mappings.get('BREAK') is not None:
                    qtmp = api_response.system_to_organization_mappings.get('BREAK')
                    set_qualifier(qtmp, 'BREAK')
                if api_response.system_to_organization_mappings.get('MEETING') is not None:
                    qtmp = api_response.system_to_organization_mappings.get('MEETING')
                    set_qualifier(qtmp, 'MEETING')
                if api_response.system_to_organization_mappings.get('TRAINING') is not None:
                    qtmp = api_response.system_to_organization_mappings.get('TRAINING')
                    set_qualifier(qtmp, 'TRAINING')

            #pprint(qualifiers)

            if len(api_response.results) > 0:

                for item in api_response.results:
                    queue_id = item.group.get('queueId')
                    media_type = item.group.get('mediaType')

                    on_queue = 0
                    off_queue = 0
                    waiting = 0
                    qualifier = ''
                    if item.data != None:
                        for data in item.data:
                            if data.metric == 'oOnQueueUsers' and data.qualifier is not None:
                                qualifier = data.qualifier
                            if data.metric == 'oOffQueueUsers' and data.qualifier is not None:
                                qualifier = qualifiers[data.qualifier]

                            #print(f'queue: {queues[queue_id]}, media: {media_type}, metric: {data.metric}, qualifier: {qualifier}, count: {data.stats.count}')
                            if data.stats.count > 0:

                                # Create an array of points with tags and fields.
                                points.append(Point("pqa")
                                    .tag("name", queues[queue_id])
                                    .tag("media", media_type)
                                    .tag("metric", data.metric)
                                    .tag("qualifier", qualifier)
                                    .field("count", data.stats.count)
                                )


        except ApiException as e:
            print("Exception when calling RoutingApi->post_analytics_queues_observations_query: %s\n" % e)


    except ApiException as e:
        print("Exception when calling RoutingApi->get_routing_queues: %s\n" % e)

    page_number += 1
    if page_number > api_response1.page_count:
        break

    #sys.exit()

if len(points) > 0:
    print(f'data len: {len(points)}')
    with InfluxDBClient3(host=host,
        token=token,
        database=database,
        write_client_options=wco) as client:
            client.write(points, write_precision='s')


