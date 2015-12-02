# coding:utf-8

from pykafka.client import KafkaClient
import logging

logging.basicConfig(level = logging.INFO)

offset_check_logger = logging.getLogger('offset_check')

client = KafkaClient('localhost:8990,localhost:8991,localhost:8992')

nmq = client.topics['nmq']

offsets = nmq.latest_available_offsets()

offset_check_logger.info('消息总量如下:')

for partition, item in offsets.items():
    offset_check_logger.info('[partition={} offset={}]'.format(partition, item.offset[0]))
    
partitions = offsets.keys()
    
offset_check_logger.info('消息读取量如下:')

offset_manager = client.cluster.get_offset_manager('balance-consumer')
