# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from time import sleep
from json import dumps, loads

from kafka.structs import (
    TopicPartition
)
import collection as collection
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
import statistics
from scipy import stats
# Press the green button in the gutter to run the script.



if __name__ == '__main__':


    #t = 'transactionv0'
    t = 'inferencev2'
    bootstrap_servers = []
    consumer = KafkaConsumer(

        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        enable_auto_commit=True,
        group_id=None,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    tls = [TopicPartition(t, 0),TopicPartition(t, 1),TopicPartition(t, 2),TopicPartition(t, 3),TopicPartition(t, 4),TopicPartition(t, 5),TopicPartition(t, 6),TopicPartition(t, 7)]
    print('here')
    consumer.assign(tls)
    now = datetime.now()
    print('{}'.format(now - now))
    i = 0
    durs = []
    for message in consumer:
        message = message.value
        durs.append(message['dur_evt_inf'])
        i = i + 1
        if(i%1000==0):
            print(message)
            now2 = datetime.now()
            print(i)
            print('{}'.format(now2 - now))
    now2 = datetime.now()
    print(i)
    print('{}'.format(now2 - now))

    mean = statistics.mean(durs)
    median = statistics.median(durs)
    max = max(durs)
    min = min(durs)

    print('max=' + str(max))
    print('min=' + str(min))
    print('avg=' + str(mean))
    print('avg_adj=' + str(mean-min))
    print('med=' + str(median))
    print('adj_max=' + (max-min))
    print('adj_min=' + 1)

    print('size' + str(len(durs)))
    consumer.close()



