# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from time import sleep
from json import dumps, loads

import collection as collection
from confluent_kafka import Producer, SerializingProducer
import csv
import time
import json

import socket

# Press the green button in the gutter to run the script.
from confluent_kafka.serialization import StringSerializer

if __name__ == '__main__':
    user='<USER>'
    pwd='<PASS>'
    bsts='pkc-4kgmg.us-west-2.aws.confluent.cloud:9092'
    topic = 'T1'
    conf = {'bootstrap.servers': bsts,
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': '/tmp/cacert.pem',
            'sasl.username': user,
            'sasl.password': pwd,
            #'key.serializer': StringSerializer('utf_8'),
            #'value.serializer': StringSerializer('utf_8'),

            'client.id': 'test-sw-1'}

    producer = Producer(conf)

    print('test')
    with open('./creditcard.csv', encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        #"Time", "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19",
        # "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount", "Class"
        # Convert each row into a dictionary
        # and add it to data
        i=0
        for rows in csvReader:
            i = i + 1
            data={}
            # Assuming a column named 'No' to
            # be the primary key
            #key = rows['Time']
            data['time'] = rows['Time']
            data['message_id'] = i
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
            data['ignore'] = False

            i = i + 1
            k = str(i);
            v = json.dumps(data).encode('utf-8')
            print(v)
            producer.produce(topic, value=v, key=k)
            i=i+1
            k = str(i);
            v = json.dumps(data).encode('utf-8')
            print(v)
            producer.produce(topic, value=v, key=k)

            i = i + 1
            k = str(i);
            v = json.dumps(data).encode('utf-8')
            print(v)
            producer.produce(topic, value=v, key=k)

            i = i + 1
            k = str(i);
            v = json.dumps(data).encode('utf-8')
            print(v)
            producer.produce(topic, value=v, key=k)
            #producer.flush()
            '''
            i = i+1
            partition = i % 8
            data['message_id'] = i
            print(partition)
            producer.send(topic, value=data,  key=str(i))
            i = i + 1
            partition = i % 8
            data['message_id'] = i
            print(partition)
            producer.send(topic, value=data,  key=str(i))
            i = i + 1
            partition = i % 8
            data['message_id'] = i
            print(partition)
            producer.send(topic, value=data, key=str(i))
            '''
            if(i%10000==0):
                print('now flushing')
                producer.flush()
            if(i==2500000):
                break;
            #print(data)
            print(i)

    producer.flush()
    producer.close()
    print('done')
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
