#!/usr/bin/env python
#
# MAINTAINER: panick@
# 
#

"""Simple tool for transcoding media objects stored in Google Cloud Storage (GCS)

This tool fetches events from Google Cloud Pub/Sub that specify objects in GCS that should be
transcoded into different bitrates, formats, etc. It expects the payloads in Pub/Sub to adhere
to what is expected from GCS -> Pub/Sub notifications:

https://cloud.google.com/storage/docs/pubsub-notifications
"""

from google.cloud import pubsub_v1

import json
import logging
import os
import time

PROJECT_ID="panick-sandbox-bro"
SUBSCRIPTION_NAME="k8s-pull"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    PROJECT_ID, SUBSCRIPTION_NAME
)
future = subscriber.subscribe(subscription_path, callback)


def callback(message):
    """Callback function to handle processing of event
        Put in here all the THINGS you need to do :)
    """
    
    print("You are inside the call-back...")
    print("Processing message: "+message.message_id)
    # Pull out data dictionary and store as data_block (json)
    data_block = json.loads(message.data.decode())
    print(data_block)
    # object_uri is link to GCS object
    object_uri = data_block["selfLink"]
    print(object_uri)
    ffmpeg_process(object_uri)              # Call ffmpeg_process helper to process
    print("Sleeping for 1 second...")
    time.sleep(1)
    message.nack() # This would not ack the message

#   message.ack() # This would ack the message


def ffmpeg_process(gcs_uri):
    """ Transcode the new object with ffmpeg
    """
    print("You are inside ffmpeg_process()...")
    print("Processing object: " + gcs_uri)
    pass

def get_event():
    """ Pull event from Pub/Sub subscription
    """
    pass


def main():
    callback()


if __name__ == '__main__':
    main()