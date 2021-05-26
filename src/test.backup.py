import os
import ssl
import time
import pytz
import random
import datetime
import logging
import argparse
import threading

import jwt
import paho.mqtt.client as mqtt

# global variables

logger = logging.getLogger(__name__)

tz = pytz.timezone('America/Santiago')
tz = pytz.timezone('UTC')

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
        'creating JWT using %s from private key file %s',
        algorithm,
        private_key_file
    )

    return jwt.encode(token, private_key_str, algorithm=algorithm)


# mqtt methods


def error_str(rc):
    return '{}: {}'.format(rc, mqtt.error_string(rc))


def on_connect(unused_client, unused_userdata, unused_flags, rc):
    logger.info('on_connect => %s', mqtt.connack_string(rc))


def on_disconnect(unused_client, unused_userdata, rc):
    logger.info('on_connect => %s', error_str(rc))


def on_publish(unused_client, unused_userdata, unused_mid):
    logger.info('on_publish')


def on_message(unused_client, unused_userdata, message):
    payload = str(message.payload.decode('utf-8'))
    logger.info(
        'on_message => \'%s\' on topic \'%s\' with Qos %s',
        payload, 
        message.topic, 
        str(message.qos)
    )

# mqtt client

def get_client(
    project_id,
    cloud_region,
    registry_id,
    device_id,
    private_key_file,
    algorithm,
    ca_certs,
    mqtt_bridge_hostname,
    mqtt_bridge_port
):

    client_id = 'projects/{}/locations/{}/registries/{}/devices/{}'.format(
        project_id, cloud_region, registry_id, device_id
    )

    logger.info('device client_id is \'%s\'', client_id)

    client = mqtt.Client(client_id=client_id)

    def _on_connect(unused_client, unused_userdata, unused_flags, rc):
        on_connect(unused_client, unused_userdata, unused_flags, rc)

        mqtt_config_topic = '/devices/{}/config'.format(device_id)
        logger.info('subscribing to %s', mqtt_config_topic)
        client.subscribe(mqtt_config_topic, qos=1)

        mqtt_command_topic = '/devices/{}/commands/#'.format(device_id)
        logger.info('subscribing to %s', mqtt_command_topic)
        client.subscribe(mqtt_command_topic, qos=0)

        mqtt_config_topic = '/devices/{}/config'.format('sensor')
        logger.info('subscribing to %s', mqtt_config_topic)
        client.subscribe(mqtt_config_topic, qos=1)

        mqtt_command_topic = '/devices/{}/commands/#'.format('sensor')
        logger.info('subscribing to %s', mqtt_command_topic)
        client.subscribe(mqtt_command_topic, qos=0)


    client.on_connect = _on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    username = 'unused'
    password = create_jwt(project_id, private_key_file, algorithm)
    client.username_pw_set(username=username, password=password)

    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    logger.info('connecting to %s:%s', mqtt_bridge_hostname, mqtt_bridge_port)
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    return client


if __name__ == '__main__':

    default_loglevel = 'INFO'

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--loglevel',
        default=default_loglevel
    )

    args = parser.parse_args()

    logging.basicConfig(
        format='%(asctime)-15s : %(message)s', 
        level=args.loglevel.upper()
    )

    project_id = 'danarchy-io'
    cloud_region = 'us-central1'
    registry_id = 'raspberry'
    gateway_id = 'default'
    private_key_file = 'private.pem'
    algorithm = 'RS256'
    ca_certs = 'roots.pem' # https://pki.google.com/roots.pem
    mqtt_bridge_hostname = 'mqtt.googleapis.com'
    mqtt_bridge_port = 443

    devices_list = [
        'default',
        'sensor'
    ]

    devices_map = dict()
    for device in devices:
        devices_map[device] = get_client(
            project_id,
            cloud_region,
            registry_id,
            gateway_id,
            private_key_file,
            algorithm,
            ca_certs,
            mqtt_bridge_hostname,
            mqtt_bridge_port,
        )

    # gateway

    # device_id = gateway_id

    # _, event_mid = client.publish(
    #     '/devices/{}/{}'.format(device_id, 'state'), 
    #     'starting gateway at: {}'.format(time.time())
    # )
    # logger.info(event_mid)

    # _, event_mid = client.publish(
    #     '/devices/{}/{}'.format(device_id, 'events'), 
    #     'starting gateway at: {}'.format(time.time())
    # )
    # logger.info(event_mid)

    while True:
        client.loop()
        time.sleep(1)