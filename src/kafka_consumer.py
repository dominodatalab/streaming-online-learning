# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import json

from json import dumps, loads
from kafka.structs import (
    TopicPartition
)

from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
import time
import threading
class ModelInference(object):

    def __init__(self, my_id=1, bootstrap_servers=[], list_of_partitions=[], request_topic='', inference_topic='', group_id='my_grp'):
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

    def __del__(self):
        print('closing')
        #self.consumer.close()
        self.producer.flush()
        self.producer.close()

    def run(self):
        print(bootstrap_servers)
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      key_serializer=str.encode,
                                      value_serializer=lambda x:
                                      json.dumps(x).encode('utf-8'))
        self.consumer = KafkaConsumer(

            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000,
            enable_auto_commit=True,
            group_id=None,
            value_deserializer=lambda x: loads(x.decode('utf-8')))
        self.consumer.assign(self.tls)
        print('starting ' + str(self.my_id))
        now = datetime.now()
        i = 0
        while True:
            print(1)
            for message in self.consumer:
                print(message)
                message = message.value

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
                print('sending to ' + self.result_t + ' ' + str(self.result_t_p) + ' ' + str(out))

        self.producer.flush()
        self.producer.close()



if __name__ == '__main__':

    bootstrap_servers = []
    request_topic = 'transactionv0'
    inference_topic = 'inferencev102'
    example0 = ModelInference(my_id=0, bootstrap_servers=bootstrap_servers, list_of_partitions=[0,1,2,3,4,5,6,7,8],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    example0.run()
