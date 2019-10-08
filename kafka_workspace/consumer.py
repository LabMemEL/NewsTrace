from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
import mysql.connector
from mysql.connector import errorcode
from pprint import pformat
from service import producerService

def query_db(url, date_from, date_to):
    try:
        cnx = mysql.connector.connect(user='kafkaCT', password='test',
                              host='ec2-35-165-132-83.us-west-2.compute.amazonaws.com',
                              database='gdeltDB')
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
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()
    return "query not executed"


def acked_callback(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced to: %s in partition [%d] at offset %d\n" % (msg.topic(), msg.partition(), msg.offset()))

def stream_results(match):
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

def enrich_json(json_line, extras):
    enriched_line = json_line + list(extras[0])
    # print(enriched_line)
    jd = json.dumps(enriched_line).encode('utf-8')
    return jd

def get_range(ts):
    day_to = ts[6:8]
    day_from = max(int(day_to)-7, 0)
    if day_from < 10:
        return "0"+str(day_from), day_to
    return str(day_from), day_to


if __name__ == '__main__':

    topics = ["tweets"]
    # Consumer configuration
    conf = {'bootstrap.servers': "ec2-100-20-68-249.us-west-2.compute.amazonaws.com:9092,ec2-100-20-172-143.us-west-2.compute.amazonaws.com:9092,ec2-54-148-29-178.us-west-2.compute.amazonaws.com:9092",
            'group.id': 'test_group2', 'auto.offset.reset': 'earliest'}

    # Create Consumer instance
    consumer = Consumer(conf)
    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    consumer.subscribe(topics, on_assign=print_assignment)

    try:
        while True:
            matches = []
            msg = consumer.poll(timeout=1.0)
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
                    filtered_url = jd[1][0]
                    day_from, date_to = get_range(jd[0])
                    print(day_from, date_to)
                    query_result = query_db(filtered_url, day_from, date_to)
                    if query_result:
                        # time_stamp, full_urls, full_text //  EID, URL, K_words, S_nums
                        print("found match for %s", filtered_url)
                        
                        stream_results(enrich_json(jd, query_result))

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()



# def update_db(matches):
#     try:
#         cnx = mysql.connector.connect(user='kafkaCT', password='test',
#                               host='ec2-35-165-132-83.us-west-2.compute.amazonaws.com',
#                               database='RESULT')
#         cursor = cnx.cursor()
#         update = ("INSERT INTO MATCHES "
#             "VALUES(%s, %s, %s, %s, %s)", matches)
#             # format(matches["TS"], matches["URL"], matches["Full_text"], matches["TK_words"], matches["EID"])
#         rs = cursor.executemany(update)
#         return rs
#     except mysql.connector.Error as err:
#         if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#             print("Something is wrong with your user name or password")
#         elif err.errno == errorcode.ER_BAD_DB_ERROR:
#             print("Database does not exist")
#         else:
#             print(err)
#     else:
#         cnx.close()
#     return "query not executed"