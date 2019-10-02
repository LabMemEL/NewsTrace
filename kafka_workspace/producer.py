from confluent_kafka import Producer
import socket
import sys
import random #temp import for key producing test
import json
import time
from time import sleep
if __name__ == '__main__':

    def acked_callback(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced to: %s in partition [%d] at offset %d\n" % (msg.topic(), msg.partition(), msg.offset()))

    # extract needed fields from jsonl tweets
    def extract_url(json_line):
        json_parsed = json.loads(json_line)
        json_urls = json_parsed["entities"]["urls"]

        full_urls = []
        full_text = json_parsed["full_text"]
        time_stamp = time.strftime('%Y%m%d %H:%M:%S', time.strptime(json_parsed["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
        print("sending tweet posted at "+ time_stamp)
        for json_url in json_urls:
            full_urls.append(json_url["expanded_url"])
        # generate json dump
        jd = json.dumps([time_stamp, full_urls, full_text]).encode('utf-8')
        return jd

    def produce_message():
        conf = {'bootstrap.servers': "ec2-100-20-68-249.us-west-2.compute.amazonaws.com:9092,ec2-100-20-172-143.us-west-2.compute.amazonaws.com:9092,ec2-54-148-29-178.us-west-2.compute.amazonaws.com:9092",
                'client.id': socket.gethostname(),
                'default.topic.config': {'acks': 'all'}}
        topic = "test_topic3"
        producer = Producer(conf)

        # read twitter data and filter
        with open('tweets_1.jsonl', 'r') as json_lines:
            num_sent = 0
            for line in json_lines:
                json_message = extract_url(line) #consider including other expanded urls if needed
                sleep(0.5)
                try:
                    # produce line by line, messages need to be encoded into bytes
                    rand_key = random.randint(1001,2001)
                    producer.produce(topic, key=str(rand_key), value=json_message, callback=acked_callback)
                    num_sent += 1
                except BufferError:
                    sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % 
                    len(producer))
                producer.poll(0.5) 
                if (num_sent >= 1000):
                    break
        #wait long enough for events. if acked there will be call back

        producer.flush()

    produce_message()
