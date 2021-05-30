"""
QoS Messaging:
 - 0  No guarantee (best effort only)
      even when the request returns OK
 - 1  At-least-once delivery guaranteed 
      if the sendCommandtoDevice request returns OK
"""

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

logging.basicConfig(
    format='%(asctime)-15s %(name)s [%(levelname)s] %(threadName)s:%(funcName)s:%(lineno)d : %(message)s'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

basepath = pathlib.Path(__file__).resolve().absolute().parent

# check imports

try:
    import pytz
    import jwt
    import ssl
    import paho.mqtt.client as mqtt
    #import Adafruit_DHT as adafruit
    # import cv2

    # from google.cloud import storage
except:
    logging.exception(
        'missing requirements, install %s', 
        basepath.joinpath('requirements.txt').as_posix()
    )
    sys.exit(1)

# timezone

tz = pytz.timezone('America/Santiago')
tz = pytz.timezone('UTC')

# mqtt

connection_project = 'danarchy-io'
connection_region = 'us-central1'
connection_registry = 'raspberry'
connection_gateway = 'default'
connection_key = None
connection_devices = None
connection_publish_mid = None


# connection

connection_client = None
connection_timeout = 20
connection_connected = False
connection_connected_ts = None
connection_expire = None

# events

connection_event_connected = threading.Event()
connection_event_disconnected = threading.Event()

# locks

lock_connection = threading.RLock()
lock_commands = threading.RLock()
lock_configuration = threading.RLock()
lock_attach = threading.RLock()

# threads

thread_connection = None
thread_gateway_state = None

# dicts

connection_publish_mid = None

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

def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error('%s'.format(arg))
    else:
        return arg

def publish(topic, payload, qos=0):
    with lock_connection:
        try:
            if connection_connected:
                _, mid = connection_client.publish(topic, payload, qos=qos)
                logger.info(
                    'published on {}, mid {}, payload = {}'.format(
                        topic, mid, payload
                    )
                )
                return True, mid
            else:
                logger.warning(
                    'connected = {}, error publishing on {}, payload = {}'.format(
                        connection_connected, topic, payload
                    )
                )
                return False, -1
        except:
            logger.exception(
                'connected = {}, error publishing on {}, payload = {}'.format(
                    connection_connected, topic, payload
                )
            )
            return False, -1

# defaul mqtt callbacks

def error_str(rc):
    return '{}: {}'.format(rc, mqtt.error_string(rc))

def callback_connect(client, userdata, unused_flags, rc):
    global connection_connected
    global connection_connected_ts

    logger.info('callback_connect => %s', mqtt.connack_string(rc))

    with lock_connection:
        connection_event_disconnected.clear()
        connection_connected_ts = datetime.datetime.now(tz)
        connection_connected = True
        setup_devices()
        setup_threads()

def callback_disconnect(client, userdata, rc):
    logger.info('callback_disconnect => %s', error_str(rc))
    with lock_connection:
        connection_event_disconnected.set()

def callback_subscribe(client, userdata, mid, granted_qos):
    logger.debug('callback_subscribe => mid {}, qos {}'.format(mid, granted_qos))

def callback_publish(client, userdata, mid):
    pass

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

# thread functions

def thread_connection_loop():
    connection_client.loop_forever()
    logger.info('exiting...')

def thread_loop_gateway_state(topic):
    while True:
        payload = 'ping {}'.format(str(datetime.datetime.now(tz)))
        success, mid = publish(topic, payload, 0)
        time.sleep(2)

# callbacks: gateway

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

# callbacks: sensor

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

# setups

def setup_connect():
    global connection_client
    global thread_connection

    logger.info('starting mqtt client...')

    connection_client = build_client(
        connection_project, 
        connection_region, 
        connection_registry, 
        connection_gateway, 
        connection_key,
        callback_connect=callback_connect,
        callback_disconnect=callback_disconnect,
        callback_publish=callback_publish,
        callback_subscribe=callback_subscribe,
        callback_message=callback_message
    )

    thread_connection = threading.Thread(
        name='thread_connection',
        target=thread_connection_loop, 
    )
    thread_connection.start()

def setup_disconnect():
    global connection_client
    global connection_connected

    with lock_connection:
        if connection_client and connection_connected:
            logger.info('detaching devices from the gateway...')
            devices = {
                k:v for k,v in connection_devices.items() if k != connection_gateway 
            }

            for device in devices:
                mid = setup_detach(connection_client, device)
            
            time.sleep(5)

            connection_connected = False
            connection_client.disconnect()

        if not connection_event_disconnected.is_set():
            logger.info('waiting on desconnection to complete...')
            connection_event_disconnected.wait(timeout=connection_timeout)
            if not connection_event_disconnected.is_set():
                logger.error('disconnection timeout')


def setup_devices():
    global connection_devices
    global connection_publish_mid
    global barrier_connection_devices

    logger.info('starting devices configuration..')

    logger.info('attaching devices to the gateway...')
    devices = {
         k:v for k,v in connection_devices.items() if k != connection_gateway 
    }

    logging.debug('devices => %s', connection_client)
    
    # connection_publish_mid = dict()
    # barrier = threading.Barrier(len(devices) + 1, timeout=10)

    for device in devices:
        mid = setup_attach(connection_client, device)
    
    time.sleep(5)
    
    for device, subtopics in devices.items():
        for subtopic, configuration in subtopics.items():
            qos = configuration['qos']
            callback = configuration['callback']
            setup_subscribe(connection_client, device, qos, subtopic, callback)

def setup_subscribe(client, device, qos, subtopic, callback):
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

def setup_threads():
    global thread_gateway_state

    # gateway state

    if not thread_gateway_state:
        thread_gateway_state = threading.Thread(
            name='thread_gateway_state',
            target=thread_loop_gateway_state, 
            args=('/devices/{}/{}'.format(connection_gateway, 'state'),)
        )
        thread_gateway_state.start()

def setup_attach(client, device, auth=''):
    topic = "/devices/{}/attach".format(device)
    _, mid = client.publish(topic, '{{"authorization" : "{}"}}'.format(auth), qos=1)
    logger.info('attaching => %s to %s with mid %s', device, topic, mid)
    return mid

def setup_detach(client, device):
    topic = "/devices/{}/detach".format(device)
    _, mid = client.publish(topic, '{}', qos=1)
    logger.info('detaching => %s to %s with mid %s', device, topic, mid)
    return mid

# main

if __name__ == '__main__':

    default_loglevel = 'INFO'

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--loglevel',
        metavar='INFO',
        default=default_loglevel
    )

    parser.add_argument(
        '--private-key',
        required=True,
        dest='key',
        help='private key file path',
        metavar='/absolute/path/private.pem',
        type=lambda x: is_valid_file(parser, x),
        default=default_loglevel
    )

    parser.add_argument(
        '--expire',
        help='minutes for the jwt to expire, triggers a reconnection',
        metavar='20',
        type=int,
        default=60
    )

    args = parser.parse_args()

    logger.info('args => {}'.format(args))

    connection_devices = OrderedDict({
        connection_gateway : {
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

    logger.setLevel(args.loglevel.upper())

    connection_key = args.key
    connection_expire = args.expire


    setup_connect()
    time.sleep(connection_expire)
    setup_disconnect()
    time.sleep(5)
    setup_connect()