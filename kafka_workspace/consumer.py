from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
import mysql.connector
from mysql.connector import errorcode
from pprint import pformat

def filter_domain(url):
    return url

def query_db(url, date):
    try:
        cnx = mysql.connector.connect(user='kafkaCT', password='test',
                              host='ec2-35-165-132-83.us-west-2.compute.amazonaws.com',
                              database='gdeltDB')
        cursor = cnx.cursor()
        query = ("SELECT EID, URL, K_words, S_nums FROM RS_201508_3 "
         "WHERE Day BETWEEN '11' AND '14' "
         "AND URL= '" + url + "' "  
         "LIMIT 3;")
        cursor.execute(query)
        rs = cursor.fetchall()
        # possiblely multiple, only showing first one
        for row in rs:
            print("\n\nfound a match!")
            print(row)
            print("\n\n")
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

if __name__ == '__main__':
    # optlist, argv = getopt.getopt(sys.argv[1:], 'T:')
    # if len(argv) < 3:
    #     print_usage_and_exit(sys.argv[0])
    # broker = argv[0]
    # group = argv[1]
    # topics = argv[2:]
    topics = ["test_topic3"]


    # Consumer configuration
    conf = {'bootstrap.servers': "ec2-100-20-68-249.us-west-2.compute.amazonaws.com:9092,ec2-100-20-172-143.us-west-2.compute.amazonaws.com:9092,ec2-54-148-29-178.us-west-2.compute.amazonaws.com:9092",
            'group.id': 'test_group', 'auto.offset.reset': 'earliest'}

    # Create Consumer instance
    consumer = Consumer(conf)
    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    consumer.subscribe(topics, on_assign=print_assignment)

    try:
        while True:
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
                
                # print(jd[0], jd[1]) # comment out below to flush consume all messages

                if (jd[0] and jd[1]):
                    filtered_url = filter_domain(jd[1][0])
                    print("querying :" + filtered_url)
                    rs = query_db(filtered_url, jd[0]) 

                # url as query to db


                # show query results
                # for (EID, URL, K_words, S_nums) in rs:
                #     print("EID {:s} with key_words {:s} appeared in {:d} sources".format(
                #         EID, K_words, S_nums))

        consumer.close()

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()