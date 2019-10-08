from confluent_kafka import Producer, Consumer, KafkaException
from mysql.connector import errorcode
from pprint import pformat
import mysql.connector
import socket
import json
import time
import sys
import getopt
import logging
import config as cfg

class producerService:
    def __init__(self):
        self.conf = {'bootstrap.servers': cfg.kafkaservers,
                'client.id': socket.gethostname(),
                'default.topic.config': {'acks': 'all'}}
        self.twitterTopic = "tweets"
        self.resultTopic = "results_2"
        self.producer = Producer(self.conf)

    # extract needed fields from jsonl tweets
    def extract_json(self, json_line):
        json_parsed = json.loads(json_line)
        json_urls = json_parsed["entities"]["urls"]

        full_urls = []
        full_text = json_parsed["full_text"]
        time_stamp = time.strftime('%Y%m%d %H:%M:%S', time.strptime(json_parsed["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
        print("Sending message posted at "+ time_stamp)
        for json_url in json_urls:
            full_urls.append(json_url["expanded_url"])
        # generate json dump
        jd = json.dumps([time_stamp, full_urls, full_text]).encode('utf-8')
        return jd

class consumerService:
    def __init__(self):   
        self.topics = ["tweets"]
        self.conf = {'bootstrap.servers': cfg.kafkaservers,
                    'group.id': 'test_group3', 'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(self.conf)
    
    def get_range(self, ts):
        day_to = ts[6:8]
        day_from = max(int(day_to)-7, 0)
        if day_from < 10:
            return "0"+str(day_from), day_to
        return str(day_from), day_to

    def query_db(self, url, date_from, date_to):
        try:
            cnx = mysql.connector.connect(user=cfg.mysql['user'],
                                        password=cfg.mysql['passwd'],
                                        host=cfg.mysql['host'],
                                        database=cfg.mysql['db'])
            cursor = cnx.cursor()
            query = ("SELECT * FROM RS_201508_3 "
            "WHERE Day BETWEEN " + date_from + " AND " + date_to + " "
            "AND URL= '" + url + "' "  
            "LIMIT 1;")
            cursor.execute(query)
            rs = cursor.fetchall()
            return rs
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Wrong user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("DB does not exist")
            else:
                print(err)
        else:
            cnx.close()
        return "query not executed"

    def stream_results(self, match):

        def acked_callback(err, msg):
            if err is not None:
                print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            else:
                print("Message produced to: %s in partition [%d] at offset %d\n" % (msg.topic(), msg.partition(), msg.offset()))
        
        ps = producerService()
        p_result = ps.producer
        topic = ps.resultTopic
        try:
            p_result.produce(topic, value=match, callback=acked_callback)
        except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % 
                len(p_result))
        p_result.poll(0.5) 
        p_result.flush()

    def enrich_json(self, json_line, extras):
        enriched_line = json_line + list(extras[0])
        # print(enriched_line)
        jd = json.dumps(enriched_line).encode('utf-8')
        return jd
# append(jd[0], filtered_url, jd[2], query_result["K_words"], query_result["EID"])