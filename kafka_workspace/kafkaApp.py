from service import producerService, consumerService
from time import sleep
import random #temp import for key producing test
import sys
import json

def acked_callback(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced to: %s in partition [%d] at offset %d\n" % (msg.topic(), msg.partition(), msg.offset()))


def stream_tweets():
    ps = producerService()
    p_twitter = ps.producer
    topic = ps.twitterTopic

    with open('tweets_1.jsonl', 'r') as json_lines:
        num_sent = 0
        for line in json_lines:
            json_message = ps.extract_json(line) #consider including other expanded urls if needed
            sleep(0.1)
            try:
                # produce line by line, messages need to be encoded into bytes
                rand_key = random.randint(1001,2001)
                p_twitter.produce(topic, key=str(rand_key), value=json_message, callback=acked_callback)
                num_sent += 1
            except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % 
                len(p_twitter))
            p_twitter.poll(0.1) 
            if (num_sent >= 15000):
                break
    p_twitter.flush()

def consume_tweets():
    # Consumer configuration

    # Create Consumer instance
    cs = consumerService()
    c_twitter = cs.consumer
    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    c_twitter.subscribe(cs.topics, on_assign=print_assignment)

    try:
        while True:
            matches = []
            msg = c_twitter.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                # load from partitions, returned jd is a list
                jd = json.loads(msg.value().decode('utf-8'))

                if (jd[0] and jd[1]):
                    parimary_url = jd[1][0]
                    day_from, date_to = cs.get_range(jd[0])
                    query_result = cs.query_db(parimary_url, day_from, date_to)
                    if query_result:
                        # time_stamp, full_urls, full_text //  EID, URL, K_words, S_nums
                        print("found match for %s", parimary_url)
                        
                        cs.stream_results(cs.enrich_json(jd, query_result))

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c_twitter.close()

if __name__ == '__main__':
    # stream_tweets()
    consume_tweets()