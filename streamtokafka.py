import os
import sys, logging
import subprocess
import socket
import pdb
import time
# from kafka.client import KafkaClient
# import kafka
from kafka import KafkaConsumer, KafkaProducer

seen_file_paths = []
# security_protocol = "SASL_PLAINTEXT"
security_protocol = "SSL"


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


# Publish the passed message to kafka
def publish_to_kafka(message, kafka_topic, kafka_broker, ssl_catfile, ssl_certificate, ssl_keyfile):
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker],
                                 security_protocol=security_protocol,
                                 ssl_check_hostname=False,
                                 ssl_cafile=ssl_catfile,
                                 ssl_certfile=ssl_certificate,
                                 ssl_keyfile=ssl_keyfile,
                                 linger_ms=10
                                 )
        producer.send(kafka_topic, message)
        producer.flush()
        print('New Message published successfully to topic :', kafka_topic)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
        sys.exit(1)


# stream the constant stream of json data to the kafka
def stream_data(topic_name, kafka_broker, data_dir, stream_freq, ca, cert, key):
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
                    if len(data) != 0:
                        publish_to_kafka(data, topic_name, kafka_broker, ca, cert, key)
                    time.sleep(stream_freq)

    except OSError as err:
        print(''.join(['Exception in stream_data: ', str(err)]))
        sys.exit(1)
    except ValueError as err:
        print(''.join(['Exception in stream_data, value error : ', str(err)]))
        sys.exit(1)
    except:
        e = sys.exc_info()[0]
        print(''.join(['Exception in stream_data:', str(e)]))
        sys.exit(1)


def main():
    if len(sys.argv) != 8:
        print(" ")
        print("Usage: streamtokafka <topic_name> <broker_ip:portno> <data_dir> <stream_freq> <ca> <cert> <key>")
        print(" ")
        print(" topic_name	  : Name of the topic to stream data")
        print(" broker_ip:portno : IP Address or URL of the kafka broker along with portno. Provide Portno ")
        print(" data_dir         : directory where the streaming files are located")
        print(" stream_freq      : Streaming Frequency in seconds ")
        print(" ca               : CA file for certificate verification ")
        print(" cert             : Client Certificate file")
        print(" key              : Client Private key ")
        return

    topic_name = sys.argv[1]
    broker_ip = sys.argv[2]
    data_dir = sys.argv[3]
    stream_frequency = sys.argv[4]
    ca = sys.argv[5]
    cert = sys.argv[6]
    key = sys.argv[7]
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.ERROR
    )
    stream_data(topic_name, broker_ip, data_dir, float(stream_frequency), ca, cert, key)


if __name__ == "__main__":
    main()
