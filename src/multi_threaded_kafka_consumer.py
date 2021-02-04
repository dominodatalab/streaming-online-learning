# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import json
from time import sleep
from json import dumps, loads
from kafka.structs import (
    TopicPartition
)
import collection as collection
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
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,batch_size=100,
                                      key_serializer=str.encode,
                                      value_serializer=lambda x:
                                      json.dumps(x).encode('utf-8'))
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            #consumer_timeout_ms=10000,
            enable_auto_commit=True,
            #consumer_group_id = None,
            #consumer_group_id = None,
            group_id=None,
            value_deserializer=lambda x: loads(x.decode('utf-8')))
        self.consumer.assign(self.tls)
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution

    def run(self):


        print('starting ' + str(self.my_id))
        now = datetime.now()

        i = 0
        while(True):
            for message in self.consumer:
                message = message.value

                ingest_ts = message['ingestTs']
                message_id = message['message_id']
                truth = message['Class']
                y_hat = truth ## Replace with model.predict & model.learn_one(
                inference_ts = time.time()
                out = {}
                out['ingest_ts'] = ingest_ts
                out['my_id'] = message_id
                out['truth'] = truth ## model.learn_one(Y,Y_HAT)
                out['y_hat'] = y_hat
                out['inference_ts'] = inference_ts


                i = i + 1
                partition = i % self.result_t_p
                #print('sending to ' + self.result_t + ' ' + str(self.result_t_p) + ' ' + str(out))
                self.producer.send(self.result_t, value=out,key=str(i))
                #self.producer.flush()
                if (i % 1000 == 0):
                    self.producer.flush()



        self.producer.flush()
        self.producer.close()



if __name__ == '__main__':
    #example = MyConsumer(my_id=0, list_of_partitions=[0,1,2,3,4, 5, 6, 7])
    #Provide kafka parameters
    bootstrap_servers = []
    request_topic = 'credit_card_v2'
    inference_topic = ' model_results_v2'
    example0 = ModelInference(my_id=0, bootstrap_servers=bootstrap_servers, list_of_partitions=[0],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    example1 = ModelInference(my_id=1, bootstrap_servers=bootstrap_servers, list_of_partitions=[1],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    example2 = ModelInference(my_id=2, bootstrap_servers=bootstrap_servers, list_of_partitions=[2],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    example3 = ModelInference(my_id=3, bootstrap_servers=bootstrap_servers, list_of_partitions=[3],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    example4 = ModelInference(my_id=4, bootstrap_servers=bootstrap_servers, list_of_partitions=[4],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    example5 = ModelInference(my_id=5, bootstrap_servers=bootstrap_servers, list_of_partitions=[5],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    example6 = ModelInference(my_id=6, bootstrap_servers=bootstrap_servers, list_of_partitions=[6],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    example7 = ModelInference(my_id=7, bootstrap_servers=bootstrap_servers, list_of_partitions=[7],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')


    #example0 = ModelInference(my_id=0, bootstrap_servers=bootstrap_servers, list_of_partitions=[0,1],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    #example1 = ModelInference(my_id=1, bootstrap_servers=bootstrap_servers, list_of_partitions=[2,3],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    #example2 = ModelInference(my_id=2, bootstrap_servers=bootstrap_servers, list_of_partitions=[4,5],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    #example3 = ModelInference(my_id=3, bootstrap_servers=bootstrap_servers, list_of_partitions=[6,7],request_topic=request_topic,inference_topic=inference_topic,group_id='my_grp-1')
    time.sleep(3)
    print('Checkpoint')
    time.sleep(100000)
    print('Bye')


'''
            data['time'] = rows['Time']
            data['my_id'] = i
            data['ingestTs'] = time.time()
            data['V1'] = rows['V1']
            data['V2'] = rows['V2']
            data['V3'] = rows['V3']
            data['V4'] = rows['V4']
            data['V5'] = rows['V5']
            data['V6'] = rows['V6']
            data['V7'] = rows['V7']
            data['V8'] = rows['V8']
            data['V9'] = rows['V9']
            data['V10'] = rows['V10']
            data['V11'] = rows['V11']
            data['V12'] = rows['V12']
            data['V13'] = rows['V13']
            data['V14'] = rows['V14']
            data['V15'] = rows['V15']
            data['V16'] = rows['V16']
            data['V17'] = rows['V17']
            data['V18'] = rows['V18']
            data['V19'] = rows['V19']
            data['V20'] = rows['V20']
            data['V21'] = rows['V21']
            data['V22'] = rows['V22']
            data['V23'] = rows['V23']
            data['V24'] = rows['V24']
            data['V25'] = rows['V25']
            data['V26'] = rows['V26']
            data['V27'] = rows['V27']
            data['V28'] = rows['V28']
            data['V28'] = rows['V28']
            data['Amount'] = rows['Amount']
            data['Class'] = rows['Class']

'''