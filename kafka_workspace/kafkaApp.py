from service import producerService
from time import sleep
import random #temp import for key producing test

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

if __name__ == '__main__':
    stream_tweets()