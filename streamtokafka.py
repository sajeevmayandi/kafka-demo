import os
import sys,logging
import subprocess
import socket
import pdb
import time
#from kafka.client import KafkaClient
#import kafka 
from kafka import KafkaConsumer, KafkaProducer

seen_file_paths = []

def get_formatted(file_path):
    p = subprocess.Popen(
        ['cat', file_path],
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    (std_out, std_err) = p.communicate()
    if p.returncode == 0:
        return std_out
    else:
        return ""



#Publish the passed message to kafka
def publish_to_kafka(message,kafka_topic,kafka_broker):
    producer = None
    try:
       producer = KafkaProducer(bootstrap_servers=[kafka_broker], api_version=(0, 10),linger_ms=10)
       producer.send(kafka_topic,message)
       producer.flush()
       print('Message published successfully.')
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))



#stream the constant stream of json data to the kafka
def stream_data(topic_name, kafka_broker, data_dir, stream_freq):
    try:
        files = os.listdir(data_dir)
        files.sort()
        for f in files:
            file_path = ''.join([data_dir, '/', f])
            if not 'current' in f and not file_path in seen_file_paths:
                seen_file_paths.append(file_path)
                if os.path.isfile(file_path):
                    print('Processing file {}'.format(file_path))
                    data = get_formatted(file_path)
                    print(len(data))
                    if len(data) != 0:
                        publish_to_kafka(data,topic_name, kafka_broker)
                    time.sleep(stream_freq)

    except OSError as err:
        print (''.join(['Exception in stream_data: ',str(err)]))
    except ValueError as err:
        print (''.join(['Exception in stream_data, value error : ', str(err)]))
    except:
        e = sys.exc_info()[0]
        print (''.join(['Exception in stream_data:', str(e)]))


def main():
    if len(sys.argv) != 5:
        print ("Usage: streamtokafka <topic_name> <broker_ip> <netflow_files_dir> <stream_frequency_seconds>")
        return

    topic_name = sys.argv[1]
    broker_ip = sys.argv[2]
    data_dir = sys.argv[3]
    stream_frequency = sys.argv[4]
    logging.basicConfig(
 	   format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
	    level=logging.INFO
    )
    stream_data(topic_name, broker_ip, data_dir, float(stream_frequency))



if __name__ == "__main__":
    main()

