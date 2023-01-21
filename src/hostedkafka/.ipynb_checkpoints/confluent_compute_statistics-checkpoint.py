import sys
from json import loads

from confluent_kafka import Consumer
from confluent_kafka.cimpl import KafkaError, KafkaException

from confluent_kafka import TopicPartition
import datetime
import statistics
user = '<USER>'
pwd = '<PASS>'
bsts = 'SERVER:PORT'
t = <TOPIC_NAME>
conf = {'bootstrap.servers': bsts,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': user,
        'sasl.password': pwd,
        'ssl.ca.location': '/tmp/cacert.pem',
        'group.id': <PROVIDE_A_UNIQUE_VALUE_FOR_EACH_RUN>,
        'auto.offset.reset': 'smallest'}
running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.assign(topics)
        durs = []
        i=0
        message = {}
        while running:

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
                #print(message)
                if not message['dur_evt_inf'] is None:
                    i = i + 1
                    durs.append(message['dur_evt_inf'])
            if(i==1000000):
                break
            #durs.append(m['dur_evt_inf'])

            if (i % 1000 == 0):
                print(message)
                #now2 = datetime.now()
                print(i)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        #print(durs)
        mean = statistics.mean(durs)
        median = statistics.median(durs)
        max1 = max(durs)
        min2 = min(durs)

        print('max=' + str(max1))
        print('min=' + str(min2))
        print('avg=' + str(mean))
        print('med=' + str(median))
        print('total obs =' + str(len(durs)))
def shutdown():
    running = False
consumer = Consumer(conf)

tls = [TopicPartition(t, 0),TopicPartition(t, 1),TopicPartition(t, 2),TopicPartition(t, 3),
       TopicPartition(t, 4),TopicPartition(t, 5),TopicPartition(t, 6),TopicPartition(t, 7)]
basic_consume_loop(consumer,tls)
