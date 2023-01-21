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

if __name__ == '__main__':
    p_ptn = int(sys.argv[1])
    print(p_ptn)
    user= os.environ['kafka_username']
    password= os.environ['kafka_password']
    bsts= os.environ['kafka_bootstrap_servers']
    topic = 'f2'
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
    k = 0
    size=100000
    
    data = dataset.take(size)
    for x,y in data:        
        if(k%1000==0):
            print(f"now processing {k}")
        k = k + 1
        data = {}
        data['f']=x
        data['y']=y
        v= json.dumps(data).encode('utf-8')
        if k%4==p_ptn:            
            producer.produce(topic, value=v, key=str(k))


        if k%400==0:           
            if k%4000==0:
                print(f'flushing {k}')
            producer.flush()


    producer.flush()
    producer.close()
    print('done')
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
