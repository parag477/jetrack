
"""

Sample client for Flightradar24 position feed.

The client allows testing connectivity to the position feed,
but requires changes for production data consumption.

The code was developed and tested under Python 3.9.1.

List of things to consider if developing a production client based on this file:
- checkpointing should be used:
  - Azure python library supports this out of the box for checkpoints stored in Azure.
    This client has an example for this, see variables STORAGE_ACCOUNT_NAME, STORAGE_KEY
    and LEASE_CONTAINER_NAME.
  - Checkpointing can be implemented without Azure, see DummyStorageCheckpointLeaseManager.
  - Make sure to keep track of checkpoint on a per partition basis.
- optionally proxy configuration should be specified if available
- partitions should be considered.
  - The azure python library (used here) automatically reads from all partitions.
  - If developing a custom solution, care may be needed to read from all partitions.
- add code to handle timeouts and reconnect upon failures.
  - See the used azure module for potential exceptions.
  - Double check per partition checkpointing for per partition reconnects.
"""

import json
import logging
import os
import sys
import zlib
import gzip
import atexit
import signal
from amqp_consumer import AMQPConsumer

import requests
from pymongo import MongoClient

mongo_url = "mongodb://localhost:27017/"


# --- Azure Event Hub credentials to access Flightradar24 Live Event feed -------------------------
# -------------------------------------------------------------------------------------------------
# --- Credentials below will be provided by Flightradar24 -----------------------------------------


# sb://<FQDN>/;SharedAccessKeyName=<KeyName>;SharedAccessKey=<KeyValue>
CONNECTION_STRING = 'Endpoint=sb://fr24-events-feed-3-westeurope.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=RuJg0AfiHMRK9lVCEPia2eybYvCA7oWXM+AEhHzHTlA=;EntityPath=tradefinex'
CONSUMER_GROUP = os.environ.get('EVENT_HUB_CONSUMER_GROUP', '$Default')
# -------------------------------------------------------------------------------------------------
# --- Optional, if set can be used to store queue offset in Azure cloud storage -------------------
STORAGE_CONNECTION_STR = os.environ.get('STORAGE_CONNECTION_STR', '')
BLOB_CONTAINER_NAME = os.environ.get('BLOB_CONTAINER_NAME', '')
# -------------------------------------------------------------------------------------------------
# --- Optional, if you're behind corporate firewall or proxy --------------------------------------
PROXY_HOSTNAME = os.environ.get('PROXY_HOSTNAME', '')
PROXY_PORT = os.environ.get('PROXY_PORT', '')
PROXY_USER = os.environ.get('PROXY_USER', '')
PROXY_PASS = os.environ.get('PROXY_PASS', '')
# --- Client name can be chosen by you to facilitate debugging if needed --------------------------
CLIENTNAME = os.environ.get('CLIENTNAME', 'python-exabbvbmple-consumer')


# def on_receive_callback(gzip_data):
    
#     print('ReceivedCallback', gzip_data)
#     with open('flight_data.txt', 'a') as file:
#       json.dump(json.loads(gzip_data), file)
#       file.write('\n')

#     content = read_content(gzip_data)

#     print('ReceivedCallback:', content)

#     # Save the received data to a file
file_path = 'C:\\Users\\parag\\Desktop\\Flightrad\\Flightradar24-clone\\test-flight\\file_data.json'
with open(file_path, 'a') as json_file:
    json_file.write('[')


def on_receive_callback(json_data):
    try:
        print('ReceivedCallback:', json_data)

        # Parse JSON content
        content = json.loads(json_data)

        # Save the parsed content to a JSON file
        file_path = 'C:\\Users\\parag\\Desktop\\Flightrad\\Flightradar24-clone\\test-flight\\file_data.json'


        # with open(file_path, 'a') as json_file:
        #     json.dump(content, json_file, indent=2)  # Set indent to 2 for better readability
        #     json_file.write(',\n')
        #     json_file.flush()

        # data = json.dump(content, json_file, indent=2)  # Set indent to 2 for better readability
        
        client = MongoClient(mongo_url)
        db = client["flight_db"]  # Replace "yourdatabase" with your database name
        collection = db["flight_details"]  # Replace "yourcollection" wi
        
        # Insert fetched data into MongoDB
        collection.insert_one(content)


        print('ReceivedCallback (Decoded JSON):', content)

    except Exception as e:
        # Handle any errors and log them
        logging.error(f"Error in on_receive_callback: {e}")

def read_content(gzip_data):
    """
    Read compressed content, decompress and return parsed JSON
    """
    print("writing to file")



    print(f"Received {len(gzip_data)} bytes")

    # expand compressed data and check byte lengths
    json_data = zlib.decompress(gzip_data)

    # parse JSON content
    content = json.loads(json_data)

    return content


def inspect_flight(flight_id, values):
    """
    Parse list of values for a flight
    """
    # flight_id == "0" -> heartbeat message, ignore
    if flight_id == "0":
        return
    # join the field names with provided values and list them
    names = ['addr', 'lat', 'lon', 'track', 'alt', 'speed',
             'squawk', 'radar_id', 'model', 'reg', 'last_update', 'origin',
             'destination', 'flight', 'on_ground', 'vert_speed', 'callsign', 'source_type', 'eta']

    # extend when Extended Mode-S etc included
    if len(values) > len(names):
        names.append('enhanced')

    for (key, val) in zip(names, values):
        print(key, json.dumps(val))


def consume_amqp(callback=on_receive_callback):
    consumer = AMQPConsumer(connection_string=CONNECTION_STRING,
                            consumer_group=CONSUMER_GROUP,
                            storage_connection_string=STORAGE_CONNECTION_STR,
                            blob_container_name=BLOB_CONTAINER_NAME,
                            proxy_host=PROXY_HOSTNAME,
                            proxy_port=int(PROXY_PORT) if PROXY_PORT else None,
                            proxy_user=PROXY_USER, proxy_pass=PROXY_PASS)
    consumer.set_callback(callback)
    consumer.consume()



if __name__ == '__main__':
    pass
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)sZ %(message)s')
    print(__doc__)
    consume_amqp()


