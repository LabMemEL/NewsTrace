import mysql.connector
from mysql.connector import errorcode
from confluent_kafka import Consumer, KafkaException
from datetime import datetime
import sys
import getopt
import json

# datetime object containing current date and time
import logging

class sqlService:
    def __init__(self):
        self.conn = mysql.connector.connect(user='kafkaCT', password='test',
                            host='ec2-35-165-132-83.us-west-2.compute.amazonaws.com',
                            database='gdeltDB')
        self.cur = self.conn.cursor(dictionary=True)


    def query_url(self, url = 'http://www.circleid.com/posts/20150812_have_new_domains_had_our_twerking_moment/'):
        query = ("SELECT EID, URL, K_words FROM RS_201508_3 "
            "WHERE Day BETWEEN '11' AND '14' "
            "AND URL= '" + url + "' "  
            "LIMIT 1;")

        self.cur.execute(query)
        return self.cur.fetchall()

class kafkaService:
    def __init__(self):
        self.time = datetime.now()
        self.conf = {'bootstrap.servers': "ec2-100-20-68-249.us-west-2.compute.amazonaws.com:9092,ec2-100-20-172-143.us-west-2.compute.amazonaws.com:9092,ec2-54-148-29-178.us-west-2.compute.amazonaws.com:9092",
                    'group.id': self.time.strftime("%m/%d/%Y, %H:%M:%S"), 'auto.offset.reset': 'earliest'}
        self.t_topic = ["tweets"]
        self.r_tpoic = ["results_1"]
        # "results holds simplified matches, results_1 holds full message"
    
    def establishCon(self, topic):
        def print_assignment(consumer, partitions):
            print('Assignment:', partitions)
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe(topic, on_assign=print_assignment)
        
    def closeCon(self):
        self.consumer.close()

    # TODO: add domain filtering
    def filter_domain(self, full_url):
        filtered = full_url
        return filtered

    def fetch_stream(self, num = 10):
        self.establishCon(self.t_topic)
        ts = []
        while (num>0):
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                (msg.topic(), msg.partition(), msg.offset(),
                                str(msg.key())))
                # load from partitions, returned jd is a list
                jd = json.loads(msg.value().decode('utf-8'))
                if (jd[0] and jd[1]):
                    filtered_url = self.filter_domain(jd[1][0])
                    # print("querying :" + filtered_url)
                    ts.append([jd[0], filtered_url, jd[2]] )
            num -= 1
        self.closeCon()
        return ts

    def fetch_results(self, num = 1):
        self.establishCon(self.r_tpoic)
        rs = []
        attempts = 5
        while (num > 0):
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                # stop consumer when reaching end
                attempts -= 1
                print('attemtps left %d', attempts)
                if (attempts <= 0):
                    break
                continue
            if (msg and msg.error()):
                print("raising error!")
                raise KafkaException(msg.error())
            else:
                sys.stderr.write('%% Fetching Topic %s [%d] at offset %d with key %s:\n' %
                                (msg.topic(), msg.partition(), msg.offset(),
                                str(msg.key())))
                # load from partitions, returned jd is a list
                rs.append(json.loads(msg.value().decode('utf-8')))
                num -= 1
        # self.consumer.commit(async=False)
        self.closeCon()
        return rs
                # url as query to db
                # [time_stamp, full_urls, full_text]
                # possiblely multiple, only showing first one



