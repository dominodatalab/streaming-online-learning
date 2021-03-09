# This is a sample Python script.

# Press ???R to execute it or replace it with your code.
# Press Double ??? to search everywhere for classes, files, tool windows, actions, and settings.
import json
import sys 
import os

from json import dumps, loads

from confluent_kafka.cimpl import Producer, Consumer, KafkaException, KafkaError

from datetime import datetime
import time
from confluent_kafka import TopicPartition

from river import datasets
from river import evaluate
from river import metrics
from river import tree
from river import anomaly
from river import compose
from river import preprocessing

import numpy as np 
import pickle 

class ModelInference(object):
    def __init__(self, my_id=1, bootstrap_servers='', list_of_partitions=[], request_topic='', inference_topic='', group_id='my_grp'):
        """ Constructor
        :type interval: int
        :param interval: Check interval, in seconds
        """
        self.model = tree.HoeffdingTreeClassifier(max_depth=10) 
            # compose.Pipeline(
            # preprocessing.MinMaxScaler(),
            # anomaly.HalfSpaceTrees(seed=42)) 
        self.metric = metrics.ROCAUC() # metrics.Accuracy() # 
        self.my_id = my_id
        self.t = request_topic
        self.result_t = inference_topic
        self.my_grp_id = group_id
        self.result_t_p = 8
        self.bootstrap_servers = bootstrap_servers
#         self.list_of_partitions = list_of_partitions

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
                'sasl.username': 'PB4ERLTFYZ2Y3LJE',
                'sasl.password': 'QjEJg5xj4oNsRK2rxnb4N09mTUJUn1+FLnpSz1k1tWV0kJRklIWpgoLlX08ElLO+',
#                 'sasl.username': 'FFJ5LISIRJWAOBRV',
#                 'sasl.password': 'bwxVN7T4d/TeY26aSNCCelWMQN2QTtfrz+/8FC+mbvCiCriA5shQdy8rhpdTrGhu',
                # 'key.serializer': StringSerializer('utf_8'),
                # 'value.serializer': StringSerializer('utf_8'),
                
                'client.id': 'test-sw-1'}
        
        self.producer = Producer(conf)
        conf = {'bootstrap.servers': bootstrap_servers,
                'sasl.mechanism': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'sasl.username': 'PB4ERLTFYZ2Y3LJE',
                'sasl.password': 'QjEJg5xj4oNsRK2rxnb4N09mTUJUn1+FLnpSz1k1tWV0kJRklIWpgoLlX08ElLO+',
                'ssl.ca.location': '/tmp/cacert.pem',
                'group.id': group_id,
                'auto.offset.reset': 'latest'}
        self.consumer = consumer = Consumer(conf)
        self.consumer.assign(self.tls)

    def __del__(self):
        print('closing')
        #self.consumer.close()
        self.producer.flush()
        self.producer.close()

    def run(self):
        ### Set up model, metric, and starting timestamp for this model instance ###
        model = self.model
        metric = self.metric
        print('MODEL and METRIC before any messages consumed:', model, metric)
        start_consume_ts = time.time()
        print('Start Consume Time ' + str(start_consume_ts))
        ######
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
                    ingest_ts = message['ingestTs'] # Ingestion time from Kafka
                    message.pop('ingestTs')
                    message_id = message['message_id'] # Message ID from Kafka
                    message.pop('message_id')
                    truth = int(message['Class']) # float(message['Class']) # True label supplied by Kafka 
                    message.pop('Class') # Remove label remove passing the data for prediction (could combine with above)
                    message.pop('ignore')
                    x = message
                    for k,v in x.items():
                        x[k] = float(v)
                    y_hat = model.predict_one(x) # make a prediction or score # model.score_one(x) 
                    if (i % 1000 == 0): 
                        print('x: ', x)
                        for k,v in x.items():
                            print(type(x[k]))
                        print('model: ', model)
                        print('y_hat: ', y_hat)
                    inference_ts = time.time()
                    try: 
                        metric = metric.update(truth, y_hat) # update the metric
                    except: 
                        if (i % 1000 == 0): 
                            print("Not able to update metric on this iteration.")
                    model = model.learn_one(x,truth) # make the model learn # model.learn_one(x) # 
                    learn_ts = time.time()
                    out = {}
                    out['ingest_ts'] = ingest_ts
                    out['learn_ts'] = learn_ts
                    out['message_id'] = message_id
                    out['truth'] = truth 
                    out['y_hat'] = y_hat
                    out['inference_ts'] = inference_ts
                    out['dur_evt_inf'] = inference_ts - ingest_ts
                    out['dur_start_inf'] = inference_ts - start_consume_ts 
                    out['dur_inf_learn'] = learn_ts - inference_ts
                    out['model_metric'] = metric.get()
                    out['ignore'] = False
                    i = i + 1
                    # sprint(self.result_t)
                    k = str(i);
                    v = json.dumps(out).encode('utf-8')
                    self.producer.produce(self.result_t, value=v, key=k)
                    # self.producer.flush()
                    
                    if (i % 1000 == 0):
                        print('sending to ' + self.result_t + ' ' + str(self.result_t_p) + ' ' + str(out))
                        self.producer.flush()
                    if (i % 100000 == 0): 
                        try: 
                            pickle.dump( model, open( "model.pkl", "wb" ) )
                        except: 
                            os.remove("model.pkl")
                            pickle.dump( model, open( "model.pkl", "wb" ) )
                #self.producer.close()
                
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()
    

if __name__ == '__main__':
    print("List of arguments: ", sys.argv)
    # Expected Arguments: 
    # num_partitions_per_job, 
    # list_of_partitions, 
    # request_topic, 
    # inference_topic
    # group_id
    
    try: 
        num_partitions_per_job = sys.argv[1]
        print(f"User requested {num_partitions_per_job} partitions per job.")
    except: 
        num_partitions_per_job = 8 # Default
        print(f"No argument specified for num_partitions_per_job.  Using default {num_partitions_per_job} partitions.")
    
    list_of_partitions = list(map(int, sys.argv[2].strip('[]').split(','))) #[0,1,2,3,4,5,6,7] 
    print('list_of_partitions: ', list_of_partitions)
  
    bootstrap_servers = 'pkc-4kgmg.us-west-2.aws.confluent.cloud:9092'
    
    request_topic = sys.argv[3]
    inference_topic = sys.argv[4]
    group_id = sys.argv[5]
    
    example0 = ModelInference(my_id=0, bootstrap_servers=bootstrap_servers, list_of_partitions=list_of_partitions, request_topic=request_topic, inference_topic=inference_topic, group_id=group_id)
    example0.run()
    