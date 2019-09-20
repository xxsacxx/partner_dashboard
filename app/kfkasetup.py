import os
from pykafka import *

zookeeper_hosts = os.environ.get('ZOOKEEPER') or None
hosts = os.environ.get('KAFKA') or 'localhost:9092'
topic = os.environ.get('TOPIC') or b'testing'

client = KafkaClient(hosts=hosts, zookeeper_hosts=zookeeper_hosts)
topic = client.topics[topic]