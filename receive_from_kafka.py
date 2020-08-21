import os
import sys, logging
import subprocess
import socket
import pdb
import time
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from configparser import ConfigParser

# The script help get the data in the byte string from a given topic and inserts into the database
security_protocol = "SSL"


# Read the details of the database from .ini file.
def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


# function to receive the data from the given topic
def receive_from_kafka(kafka_topic, kafka_broker, ssl_cafile, ssl_certfile, ssl_keyfile):
    try:
        consumer = KafkaConsumer(kafka_topic,
                                 auto_offset_reset='earliest',
                                 bootstrap_servers=[kafka_broker],
                                 security_protocol=security_protocol,
                                 ssl_check_hostname=False,
                                 ssl_cafile=ssl_cafile,
                                 ssl_certfile=ssl_certfile,
                                 ssl_keyfile=ssl_keyfile,
                                 consumer_timeout_ms=1000)

        while True:
            message = consumer.poll(5000)
            if not message:
                print("No more messages from topic:", kafka_topic)
                break

            # Get one message at a time within last 5 seconds
            for tp, msglist in message.items():  # iterate through messages
                print("Fetched the data from topic = %s : " % tp.topic)
                for msg in msglist:
                    bytestring = msg.value
                    # the data is in bye decode and split the data based on \n
                    text = bytestring.decode('utf-8')
                    insert_to_db(text)
                print("Inserted  messages  into the database successfully:")

    except Exception as ex:
        print('Exception while Fetching  data ')
        print(str(ex))
        sys.exit(1)


# function to insert the data into the database
def insert_to_db(lines):
    try:
        conn = None
        param = config();
        conn = psycopg2.connect(**param)
        curs = conn.cursor()
       # print("Inserting data into the database, Please wait ..:", lines)

        for line in lines.split("\n"):
            if (len(line) == 0):
                continue
            SQL = "INSERT INTO kafka_data(msg) VALUES (%s);"
            data = (line,)
            curs.execute(SQL, data)
            # commit only after all the data is inserted
        conn.commit()
    except psycopg2.OperationalError as e:
        print('SQL Exception !', e)
        if (conn):
            conn.rollback()
            sys.exit(1)
    finally:
        # closing database connection.
        if (conn):
            curs.close()
            conn.close()


def main():
    if len(sys.argv) != 6:
        print(" ")
        print("Usage: receive_from_kafka  <topic_name> <broker_ip:portno> <ca> <cert> <key>")
        print(" ")
        print(" topic_name	  : Name of the topic to stream data")
        print(" broker_ip:portno : IP Address or URL of the kafka broker along with portno. Provide Portno ")
        print(" ca               : CA file for certificate verification ")
        print(" cert             : Client Certificate file")
        print(" key              : Client Private key ")
        return

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.ERROR
    )
    topic_name = sys.argv[1]
    broker_ip = sys.argv[2]
    ca = sys.argv[3]
    cert = sys.argv[4]
    key = sys.argv[5]
    receive_from_kafka(topic_name, broker_ip, ca, cert, key)


if __name__ == "__main__":
    main()
