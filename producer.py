# coding:utf-8

from pykafka.client import KafkaClient
import logging
import sys
import json
from pykafka.partitioners import hashing_partitioner

logging.basicConfig(level = logging.INFO)

producer_logger = logging.getLogger('producer')

if len(sys.argv) != 4:
    producer_logger.warning('python producer.py id url body')
    sys.exit(-1)

logging.basicConfig(level = logging.DEBUG)

client = KafkaClient('localhost:8990,localhost:8991,localhost:8992')

nmq = client.topics['nmq']

producer = nmq.get_producer(partitioner = hashing_partitioner, linger_ms = 1, sync = True)

request = {"url" : sys.argv[2], "body": sys.argv[3]}

msg = json.dumps(request)

producer.produce(msg, str(sys.argv[1]))

producer_logger.info("{} has been sent successfully~".format(msg))
