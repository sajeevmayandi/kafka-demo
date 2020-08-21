import runpy
import sys

from configparser import ConfigParser


def main():
    configure = ConfigParser()
    configure.read('test.ini')
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
        for i in range(1, int(nooftelematic)):
            file_name = data_dir + '/' + str(i) + '.txt'
            sys.argv = ['', i, file_name, nooflines]
            runpy.run_path('./generate.py', run_name='__main__')
        print("generated GPS files for  : ", nooftelematic, " telematics with  ", nooflines, "GPS fixes")

        sys.argv = ['', topic_name, broker_ip, data_dir, streamfreq, capem, cert, key]
        runpy.run_path('./streamtokafka.py', run_name='__main__')
        print("Streamed  data for all the ", nooftelematic, " telematics at topic ", topic_name)

        sys.argv = ['', schema_file]

        runpy.run_path('./config.py', run_name='__main__')
        print("Recreated the Schema")

        sys.argv = ['', topic_name, broker_ip, capem, cert, key]
        runpy.run_path('./receive_from_kafka.py', run_name='__main__')
        print("Received data for all the ", nooftelematic, "telematics at topic ", topic_name)

        sys.argv = ['', data_dir]
        runpy.run_path('./test.py', run_name='__main__')
    except:
        e = sys.exc_info()[0]
        print(''.join(['Exception in test:', str(e)]))
        exit(0)


if __name__ == "__main__":
    main()
