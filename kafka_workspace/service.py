from confluent_kafka import Producer, Consumer, KafkaException
import socket
import json
import time


class producerService:
    def __init__(self):
        self.conf = {'bootstrap.servers': "ec2-100-20-68-249.us-west-2.compute.amazonaws.com:9092,ec2-100-20-172-143.us-west-2.compute.amazonaws.com:9092,ec2-54-148-29-178.us-west-2.compute.amazonaws.com:9092",
                'client.id': socket.gethostname(),
                'default.topic.config': {'acks': 'all'}}
        self.twitterTopic = "tweets"
        self.resultTopic = "results_1"
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



# append(jd[0], filtered_url, jd[2], query_result["K_words"], query_result["EID"])