#!/usr/bin/env python

# -*- coding: utf-8 -*-

import sys
if sys.version_info[0] < 3: 
    raise Exception('python >= 3.x supported')

import os
import time
import random
import datetime
import logging
import argparse
import threading
import requests
import tempfile
import pathlib

from collections import OrderedDict

logging.basicConfig(format='%(asctime)-15s : %(message)s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

basepath = pathlib.Path(__file__).resolve().absolute().parent

try:
    import pytz
    import jwt
    import ssl
    import paho.mqtt.client as mqtt
    # import cv2

    # from google.cloud import storage
except:
    logging.exception(
        'missing requirements, install %s', 
        basepath.joinpath('requirements.txt').as_posix()
    )
    sys.exit(1)

#
# QoS Guarantee
#
#  0  No guarantee (best effort only)
#     even when the request returns OK
#
#  1  At-least-once delivery guaranteed 
#     if the sendCommandtoDevice request returns OK
#

# global variables (possibly modified by some functions)

tz = pytz.timezone('America/Santiago')
tz = pytz.timezone('UTC')

connection_event = threading.Event()

# helper functions

def create_jwt(project_id, private_key_file, algorithm):
    token = {
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=20),
        "aud": project_id,
    }

    with open(private_key_file) as file:
        private_key_str = file.read()

    logger.info(
        'creating jwt using %s from private key file %s',
        algorithm,
        private_key_file
    )

    return jwt.encode(token, private_key_str, algorithm=algorithm)

# defaul mqtt callbacks

def error_str(rc):
    return '{}: {}'.format(rc, mqtt.error_string(rc))


def callback_connect(client, userdata, unused_flags, rc):
    logger.info('callback_connect => %s', mqtt.connack_string(rc))
    connection_event.set()


def callback_disconnect(client, userdata, rc):
    logger.info('callback_disconnect => %s', error_str(rc))


def callback_subscribe(client, userdata, mid, granted_qos):
    logger.debug('callback_subscribe => mid {}, qos {}'.format(mid, granted_qos))


def callback_publish(client, userdata, unused_mid):
    logger.debug('callback_publish')


def callback_message(client, userdata, message):
    payload = str(message.payload.decode('utf-8'))
    logger.info(
        'callback_message => \'%s\' on topic \'%s\' with Qos %s',
        payload, 
        message.topic, 
        str(message.qos)
    )

# mqtt client

def build_client(
    project_id, 
    cloud_region,
    registry_id,
    device_id,
    private_key_file,
    algorithm='RS256',
    ca_certs_url='https://pki.google.com/roots.pem',
    mqtt_bridge_hostname='mqtt.googleapis.com',
    mqtt_bridge_port=443,
    callback_connect=None,
    callback_disconnect=None,
    callback_publish=None,
    callback_subscribe=None,
    callback_message=None,
):

    # build client

    client_id = 'projects/{}/locations/{}/registries/{}/devices/{}'.format(
        project_id, cloud_region, registry_id, device_id
    )

    logger.info('device client_id is \'%s\'', client_id)

    client = mqtt.Client(client_id=client_id)

    # default callbacks 

    if callback_connect:
        client.on_connect = callback_connect

    if callback_disconnect:
        client.on_disconnect = callback_disconnect
    
    if callback_publish:
        client.on_publish = callback_publish

    if callback_subscribe:
        client.on_subscribe = callback_subscribe

    if callback_message:
        client.on_message = callback_message

    # build jwt auth from private key (private.pem)

    username = 'unused'
    password = create_jwt(project_id, private_key_file, algorithm)
    client.username_pw_set(username=username, password=password)

    # download ca_cert

    res = requests.get(ca_certs_url)
    if res.status_code != 200:
        raise RuntimeError(
            '{} status code {}'.format(
                ca_certs_url, 
                res.status_code
            )
        )

    ca_certs = tempfile.mkstemp(suffix='.pem')[1]
    with open(ca_certs, 'w') as file:
        file.write(res.text)

    logger.info('ca_certs from %s is %s', ca_certs_url, ca_certs)

    # tls config

    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # connect

    logger.info('connecting to %s:%s', mqtt_bridge_hostname, mqtt_bridge_port)
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    return client

# device attach

def attach_device(client, device, auth=''):
    topic = "/devices/{}/attach".format(device)
    logger.info('attaching => %s to %s', device, topic) 
    _, mid = client.publish(topic, '{{"authorization" : "{}"}}'.format(auth), qos=1)


def detach_device(client, device):
    topic = "/devices/{}/detach".format(device)
    logger.info('detaching => %s from %s', device, topic)
    _, mid = client.publish(topic, "{}", qos=1)


def reattach_device(client, device):
    logger.info('reattach => %s', device)
    detach_device(client, device)
    attach_device(client, device)

# client loop

def client_loop_thread(client):
    client.loop_forever()

# image loop

def image_loop_thread(bucket_name, bucket_path, path='/tmp/image.jpg', video=0):
    envvar = 'GOOGLE_APPLICATION_CREDENTIALS'
    if not envvar in os.environ: 
        raise RuntimeError('envvar \'{}\' is not defined'.format(envvar))

    # setup storage client

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # setup camera

    camera = cv2.VideoCapture(video)
    camera.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

    while True:
        # take a snapshot, and save it

        value, image = camera.read()
        cv2.imwrite(path, image)

        # upload it to gcs

        blob_name = '{}/{}.jpg'.format(
            bucket_path, 
            str(datetime.datetime.now(tz))
        )
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path)

        logger.info('uploaded => gs://{}/{}'.format(bucket_name, blob_name))

    time.sleep(60)
    
# main

# specific gateway callbacks

def callback_config_gateway(client, userdata, message):
    payload = str(message.payload.decode('utf-8'))
    logger.info(
        'callback_config_gateway => \'%s\' on topic \'%s\' with Qos %s',
        payload, 
        message.topic, 
        str(message.qos)
    )

def callback_error_gateway(client, userdata, message):
    payload = str(message.payload.decode('utf-8'))
    logger.info(
        'callback_errors_gateway => \'%s\' on topic \'%s\' with Qos %s',
        payload, 
        message.topic, 
        str(message.qos)
    )

def callback_command_gateway(client, userdata, message):
    payload = str(message.payload.decode('utf-8'))
    logger.info(
        'callback_commands_gateway => \'%s\' on topic \'%s\' with Qos %s',
        payload, 
        message.topic, 
        str(message.qos)
    )

def callback_config_sensor(client, userdata, message):
    payload = str(message.payload.decode('utf-8'))
    logger.info(
        'callback_config_sensor => \'%s\' on topic \'%s\' with Qos %s',
        payload, 
        message.topic, 
        str(message.qos)
    )

def callback_error_sensor(client, userdata, message):
    payload = str(message.payload.decode('utf-8'))
    logger.info(
        'callback_error_sensor => \'%s\' on topic \'%s\' with Qos %s',
        payload, 
        message.topic, 
        str(message.qos)
    )

def callback_command_sensor(client, userdata, message):
    payload = str(message.payload.decode('utf-8'))
    logger.info(
        'callback_command_sensor => \'%s\' on topic \'%s\' with Qos %s',
        payload, 
        message.topic, 
        str(message.qos)
    )

# specific devices callbacks

# argparse helpers

def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error('%s'.format(arg))
    else:
        return arg

if __name__ == '__main__':

    default_loglevel = 'INFO'

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--loglevel',
        default=default_loglevel
    )

    parser.add_argument(
        '--private-key',
        required=True,
        dest='private_key_file',
        help='private key file path',
        metavar='/absolute/path/private.pem',
        type=lambda x: is_valid_file(parser, x),
        default=default_loglevel
    )

    args = parser.parse_args()

    # set log level

    logger.setLevel(args.loglevel.upper())

    # actual script lifecycle

    # build client

    project_id = 'danarchy-io'
    cloud_region = 'us-central1'
    registry_id = 'raspberry'
    gateway_id = 'default'
    private_key_file = args.private_key_file

    logger.info('starting IoT client..')

    client = build_client(
        project_id, 
        cloud_region, 
        registry_id, 
        gateway_id, 
        private_key_file,
        callback_connect=callback_connect,
        callback_disconnect=callback_disconnect,
        callback_publish=callback_publish,
        callback_subscribe=callback_subscribe,
        callback_message=callback_message
    )

    # run network thread and wait until connect before moving on

    client_loop = threading.Thread(
        target=client_loop_thread, 
        args=(client,)
    )
    client_loop.start()

    logger.info('waiting on connection...')
    connection_event.wait(timeout=20)
    if not connection_event.is_set():
        raise RuntimeError('connection timeout') 

    # devices configuration (including gateway)

    devices = OrderedDict({

        gateway_id : {
            'config' : {
                'qos' : 1,
                'callback' : callback_config_gateway
            },
            'errors' : {
                'qos' : 0,
                'callback' : callback_error_gateway
            },
            'commands/#' : {
                'qos' : 0,
                'callback' : callback_command_gateway
            },
        },

        'sensor' : {
            'config' : {
                'qos' : 1,
                'callback' : callback_config_sensor
            },
            'errors' : {
                'qos' : 0,
                'callback' : callback_error_sensor
            },
            'commands/#' : {
                'qos' : 0,
                'callback' : callback_command_sensor
            },
        },

    })

    logging.debug('devices => %s', devices)

    # attach all devices but the gateway

    logger.info('attaching devices to the gateway...')

    for device in devices:
        if device == gateway_id: continue
        attach_device(client, device)

    time.sleep(3)

    # subscribe all devices to its topics

    logger.info('starting device configuration..')

    logger.info('-'*80)

    for device, subtopics in devices.items():
        logger.info('device \'%s\' => configuration', device)

        for subtopic, configuration in subtopics.items():
            qos = configuration['qos']
            callback = configuration['callback']

            topic = '/devices/{}/{}'.format(device, subtopic)

            client.message_callback_add(topic, callback)
            logger.info(
                'device \'%s\' => callback %s set to %s', 
                device, topic, callback
            )

            _, mid = client.subscribe(topic, qos=qos)
            logger.info(
                'device \'%s\' => subscribe to %s QoS set to %s, with mid %s', 
                device, topic, qos, mid
            )

        logger.info('-'*80)

    # start taking camera pictures

    bucket_name = 'danarchy-io'
    bucket_path = 'iotcore/images'

    image_loop = threading.Thread(
        target=image_loop_thread, 
        args=(bucket_name, bucket_path,)
    )
    #image_loop.start()

    while True:
        #client.loop()
        time.sleep(1)