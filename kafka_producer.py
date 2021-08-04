#!/usr/bin/python
import sys, getopt
from time import sleep
import socket
import json
from confluent_kafka import Producer

# def main(argv):
conf = {'bootstrap.servers': "localhost:9092", 'client.id': socket.gethostname()}
#            "value.serializer": lambda x: json.dumps(x).encode('utf-8')}

producer = Producer(conf)

inputfile = 'data/TrainingDataSmall.txt'
outtopic = 'test-in'

file1 = open(inputfile, 'r')
Lines = file1.readlines()

for line in Lines:
    #data = json.loads(json.dumps(line).encode("utf-8"))
    #producer.produce(outtopic, value=json.loads(data))
    #The below line works the same as above. Have to see how to set value.serialzer.
    producer.produce(outtopic,value=str(line))
    producer.flush()
    print("data sent")
    sleep(10)
