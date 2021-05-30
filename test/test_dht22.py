#!/usr/bin/python

import unittest
import logging
import configparser

import Adafruit_DHT as adafruit

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] - %(message)s'
)

GPIO_PIN = None

class TestSensor(unittest.TestCase):

    def test_sensor(self):
        h,t = adafruit.read_retry(adafruit.DHT22, GPIO_PIN)
        logging.info('h = %s, t = %s', h, t)
        self.assertTrue(h is not None or t is not None)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('%s.ini' % __file__.split('.')[0])
    GPIO_PIN = config['RASPBERRY']['GPIO_DHT22']
    unittest.main()
