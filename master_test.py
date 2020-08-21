import runpy
import sys
import shutil
import os

from configparser import ConfigParser


def main():
    configure = ConfigParser()
    configure.read('test.ini')
    # The code to get the imperative configuration parameters to succced
    data_dir = configure.get('generate', 'data_dir')
    schema_file = configure.get('config', 'schema_file')

    nooftelematic = configure.get('generate', 'nooftelematic')
    nooflines = configure.get('generate', 'nooflines')
    topic_name = configure.get('stream', 'topic_name')
    broker_ip = configure.get('stream', 'broker_ip')
    streamfreq = configure.get('stream', 'streamfreq')
    capem = configure.get('stream', 'ca')
    cert = configure.get('stream', 'cert')
    key = configure.get('stream', 'key')

    # argv[0] will be replaced by runpy
    # You could also skip this if you get sys.argv populated
    # via other means
    try:
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.mkdir(data_dir)

        # Generating data set. It will be equivalent to value in nooftelematic variable
        for i in range(1, (int(nooftelematic) + 1)):
            file_name = data_dir + '/' + str(i) + '.txt'
            sys.argv = ['', i, file_name, nooflines]
            runpy.run_path('./generate.py', run_name='__main__')
        print("generated GPS files for  : ", nooftelematic, " telematics with  ", nooflines, "GPS fixes")

        # stream the data set to the given topic
        sys.argv = ['', topic_name, broker_ip, data_dir, streamfreq, capem, cert, key]
        runpy.run_path('./streamtokafka.py', run_name='__main__')
        print("Streamed  data for all the ", nooftelematic, " telematics at topic ", topic_name)

        # Recreate the schema, including all the tables etc
        sys.argv = ['', schema_file]
        runpy.run_path('./config.py', run_name='__main__')
        print("Recreated the Schema")

        # Receive the data from the given topic
        sys.argv = ['', topic_name, broker_ip, capem, cert, key]
        runpy.run_path('./receive_from_kafka.py', run_name='__main__')
        print("Received data for all the ", nooftelematic, "telematics at topic ", topic_name)

        # Finally generate the test results
        sys.argv = ['', data_dir]
        runpy.run_path('./test.py', run_name='__main__')
    except SystemExit:
        e = sys.exc_info()[0]
        print(''.join(['Exception in test:', str(e)]))
        exit(0)


if __name__ == "__main__":
    main()
