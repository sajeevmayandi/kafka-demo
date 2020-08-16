import os
import sys,logging
import subprocess
import socket
import pdb
import time
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from configparser import ConfigParser


# The script help get the data in the byte string from a given topic and inserts into the database 


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
def receive_from_kafka( kafka_topic, kafka_broker):
       consumer = KafkaConsumer(kafka_topic, auto_offset_reset='earliest',
                             bootstrap_servers=[kafka_broker], api_version=(0, 10), consumer_timeout_ms=1000)
       for msg in consumer:
          bytestring = msg.value
	  # the data is in bye decode and split the data based on \n
          text = bytestring.decode('utf-8')
          for line in text.split("\n"):
             print(line)
             insert_to_db(line)
       consumer.close()


# function to insert the data into the database
def insert_to_db(line):
   param = config();
   conn = psycopg2.connect(**param)
   curs = conn.cursor()

   try:
      SQL = "INSERT INTO kafka_data(msg) VALUES (%s);" 
      data = (line,)
      curs.execute(SQL, data) 
      conn.commit()
   except Exception:
      print( "Exception  inserting the data")
      conn.rollback()
   finally:
    #closing database connection.
        if(conn):
            curs.close()
            conn.close()

def main():
    if len(sys.argv) != 3:
        print ("Usage: receive_from_kafka  <topic_name> <broker_ip> ")
        return

    topic_name = sys.argv[1]
    broker_ip = sys.argv[2]
    receive_from_kafka(topic_name, broker_ip)



if __name__ == "__main__":
    main()

