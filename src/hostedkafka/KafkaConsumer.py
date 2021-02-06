# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import json
import sys

from json import dumps, loads

from confluent_kafka.cimpl import Producer, Consumer, KafkaException, KafkaError

from datetime import datetime
import time
from confluent_kafka import TopicPartition
class ModelInference(object):

    def __init__(self, my_id=1, bootstrap_servers='', list_of_partitions=[], request_topic='', inference_topic='', group_id='my_grp'):
        """ Constructor
        :type interval: int
        :param interval: Check interval, in seconds
        """
        self.model = None;#Create the model instance here
        self.my_id = my_id
        self.t = request_topic
        self.result_t = inference_topic
        self.my_grp_id = group_id
        self.result_t_p = 8
        self.bootstrap_servers = bootstrap_servers

        self.tls = []
        x = 0
        for i in list_of_partitions:
            self.tls.insert(x, TopicPartition(self.t, i))
            x = x+1
        #self.tls=list_of_partitions
        print(self.tls)
        conf = {'bootstrap.servers': bootstrap_servers,
                'sasl.mechanism': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'ssl.ca.location': '/tmp/cacert.pem',
                'sasl.username': '<ADD USER_NAME>',
                'sasl.password': '<ADD_PASSWORD>',
                # 'key.serializer': StringSerializer('utf_8'),
                # 'value.serializer': StringSerializer('utf_8'),

                'client.id': 'test-sw-1'}

        self.producer = Producer(conf)
        conf = {'bootstrap.servers': bootstrap_servers,
                'sasl.mechanism': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'sasl.username': 'FFJ5LISIRJWAOBRV',
                'sasl.password': 'bwxVN7T4d/TeY26aSNCCelWMQN2QTtfrz+/8FC+mbvCiCriA5shQdy8rhpdTrGhu',
                'ssl.ca.location': '/tmp/cacert.pem',
                'group.id': group_id,
                'auto.offset.reset': 'smallest'}
        self.consumer = consumer = Consumer(conf)
        self.consumer.assign(self.tls)

    def __del__(self):
        print('closing')

        self.producer.flush()
        self.consumer.close()

    def run(self):
        try:
            self.consumer.assign(self.tls)
            i = 0
            while (True):
                msg = self.consumer.poll(timeout=1.0)
                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:

                    message = loads(msg.value().decode("utf-8"))
                    ingest_ts = message['ingestTs']
                    message_id = message['message_id']
                    truth = message['Class']
                    y_hat = truth  ## Replace with model.predict & model.learn_one(
                    inference_ts = time.time()
                    out = {}
                    out['ingest_ts'] = ingest_ts
                    out['message_id'] = message_id
                    out['truth'] = truth  ## model.learn_one(Y,Y_HAT)
                    out['y_hat'] = y_hat
                    out['inference_ts'] = inference_ts
                    out['dur_evt_inf'] = inference_ts - ingest_ts;
                    i = i + 1
                    # sprint(self.result_t)
                    k = str(i);
                    v = json.dumps(out).encode('utf-8')
                    #self.producer.produce(self.result_t, value=v, key=k)
                    # self.producer.flush()

                    if (i % 2 == 0):
                        print('sending to ' + self.result_t + ' ' + str(self.result_t_p) + ' ' + str(out))
                        self.producer.flush()
                #self.producer.close()

        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()


    def runOld(self):
        print(bootstrap_servers)
        conf = {'bootstrap.servers': bootstrap_servers,
                'sasl.mechanism': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'ssl.ca.location': '/tmp/cacert.pem',
                'sasl.username': 'FFJ5LISIRJWAOBRV',
                'sasl.password': 'bwxVN7T4d/TeY26aSNCCelWMQN2QTtfrz+/8FC+mbvCiCriA5shQdy8rhpdTrGhu',
                # 'key.serializer': StringSerializer('utf_8'),
                # 'value.serializer': StringSerializer('utf_8'),

                'client.id': 'test-sw-1'}

        self.producer = Producer(conf)
        conf = {'bootstrap.servers': bootstrap_servers,
                'sasl.mechanism': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'sasl.username': 'FFJ5LISIRJWAOBRV',
                'sasl.password': 'bwxVN7T4d/TeY26aSNCCelWMQN2QTtfrz+/8FC+mbvCiCriA5shQdy8rhpdTrGhu',
                'ssl.ca.location': '/tmp/cacert.pem',
                'group.id': 'g-1',
                'auto.offset.reset': 'smallest'}
        self.consumer = consumer = Consumer(conf)
        self.consumer.assign(self.tls)
        print('starting ' + str(self.my_id))
        now = datetime.now()
        i = 0
        while True:

            for message in self.consumer:

                message = loads(msg.value().decode("utf-8"))

                ingest_ts = message['ingestTs']
                message_id = message['message_id']
                truth = message['Class']
                y_hat = truth ## Replace with model.predict & model.learn_one(
                inference_ts = time.time()
                out = {}
                out['ingest_ts'] = ingest_ts
                out['message_id'] = message_id
                out['truth'] = truth ## model.learn_one(Y,Y_HAT)
                out['y_hat'] = y_hat
                out['inference_ts'] = inference_ts
                out['dur_evt_inf'] = inference_ts - ingest_ts;
                i = i + 1
                #sprint(self.result_t)
                self.producer.send(self.result_t, value=out,key=str(message_id))
                #self.producer.flush()

                if(i%1000==0):
                    print('sending to ' + self.result_t + ' ' + str(self.result_t_p) + ' ' + str(out))
        self.producer.flush()
        self.producer.close()



if __name__ == '__main__':

    bootstrap_servers = 'pkc-4kgmg.us-west-2.aws.confluent.cloud:9092'
    request_topic = 'tv1'
    inference_topic = 'iv1'

    #1 Core - Process all 8 partitions

    #1 Core 1 Job-1.1
    #transaction2, inference2
    example0 = ModelInference(my_id=0, bootstrap_servers=bootstrap_servers, list_of_partitions=[0,1,2,3,4,5,6,7,8],
                              request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-5')
    example0.run()



    #2 Core


