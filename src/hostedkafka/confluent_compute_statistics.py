import sys
from json import loads

from confluent_kafka import Consumer
from confluent_kafka.cimpl import KafkaError, KafkaException

from confluent_kafka import TopicPartition
import datetime
import statistics
user = '<ACCESS_KEY>'
pwd = '<SECRET_KEY>'
bsts = '<BOOTSTRAP_SERVER>'
t = '<TOPIC_NAME>'
conf = {'bootstrap.servers': bsts,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': user,
        'sasl.password': pwd,
        'ssl.ca.location': '/tmp/cacert.pem',
        'group.id': 'x-2',
        'auto.offset.reset': 'smallest'}
running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.assign(topics)
        durs = []
        i=0
        message = {}
        while running:
            i = i + 1
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            message = {}
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:

                message = loads(msg.value().decode("utf-8"))
                print(message)
                durs.append(durs.append(message['dur_evt_inf']))
            if(i==1000000):
                break
            #durs.append(m['dur_evt_inf'])
            i = i + 1
            if (i % 10000 == 0):
                print(message)
                now2 = datetime.now()
                print(i)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
    mean = statistics.mean(durs)
    median = statistics.median(durs)
    max = max(durs)
    min = min(durs)

    print('max=' + str(max))
    print('min=' + str(min))
    print('avg=' + str(mean))
    print('med=' + str(median))
def shutdown():
    running = False
consumer = Consumer(conf)

tls = [TopicPartition(t, 0),TopicPartition(t, 1),TopicPartition(t, 2),TopicPartition(t, 3),
       TopicPartition(t, 4),TopicPartition(t, 5),TopicPartition(t, 6),TopicPartition(t, 7)]
basic_consume_loop(consumer,tls)