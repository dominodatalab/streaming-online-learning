# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from time import sleep
from json import dumps, loads
import os
import sys
from confluent_kafka import Producer, SerializingProducer
import csv
import time
import json
import certifi
import socket
from river import datasets
from river import metrics
from river import tree
from river import ensemble
from river import evaluate
from river import compose
from river import naive_bayes
from time import time

from river import anomaly
from river import compose
from river import datasets
from river import metrics
from river import preprocessing
import pickle
# Press the green button in the gutter to run the script.
from confluent_kafka.serialization import StringSerializer
from time import time

topic = 'HalfSpaceTrees'
sleep_time = 1
#freq = 8192 #AdaptiveRandomForestClassifier
#freq = 512 #SRPClassifierNB
#freq = 128 #SRPClassifierHAT
freq = 40000
size=102000

if __name__ == '__main__':
    topic = sys.argv[1]
    
    max_size = int(sys.argv[2])
    no_of_records_per_second = int(sys.argv[3])
    
    user= os.environ['kafka_username']
    password= os.environ['kafka_password']
    bsts= os.environ['kafka_bootstrap_servers']
    
    conf = {'bootstrap.servers': bsts,
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': certifi.where(),
            'sasl.username': user,
            'sasl.password': password,
            #'key.serializer': StringSerializer('utf_8'),
            #'value.serializer': StringSerializer('utf_8'),
            'client.id': 'test-sw-1'}

    producer = Producer(conf)    
    dataset = datasets.MaliciousURL()
    
    data = datasets.CreditCard()
    cnt = 0 
    i = 0
    print(max_size)
    while (cnt<max_size):
        for x,y in data:            
            cnt = cnt + 1
            if(cnt>max_size):
                break
          
            
            data = {}
                
            data['f']=x
            data['y']=y
            data['st']=time()            
            v= json.dumps(data).encode('utf-8')
            producer.produce(topic, value=v, key=str(cnt))
            if cnt%1000==0:           
                print(f'flushing {cnt}')
                producer.flush()            
          
            
    producer.flush()
    producer.close()
    print('done')