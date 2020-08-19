import os
import sys, logging
import subprocess
import socket
import pdb
import time
import psycopg2
from psycopg2 import Error
from psycopg2 import sql
from configparser import ConfigParser

seen_file_paths = []


# function to parse the database.ini
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


# Calculate total no of lines in the files
def total_lines_in_file(fname):
    return len(open(fname).readlines())


# function to scan the data directory
def scan_data(data_dir):
    try:
        files = os.listdir(data_dir)
        files.sort()
        lines = 0
        for f in files:
            file_path = ''.join([data_dir, '/', f])
            if not 'current' in f and not file_path in seen_file_paths:
                seen_file_paths.append(file_path)
                if os.path.isfile(file_path):
                    print('Processing file {}'.format(file_path))
                    lines = lines + total_lines_in_file(file_path)
        return lines
    except OSError as err:
        print(''.join(['Exception in scan_data: ', str(err)]))
    except ValueError as err:
        print(''.join(['Exception in scan_data, value error : ', str(err)]))
    except:
        e = sys.exc_info()[0]
        print(''.join(['Exception in scan_data:', str(e)]))


# function is used to query the table kafka_data. 
def query_table(table_name: str):
    conn = None
    try:
        param = config();
        conn = psycopg2.connect(**param)
        curs = conn.cursor()

        with curs as cursor:
            stmt = sql.SQL("""
            SELECT
                count(*)
            FROM
                {table_name}
            """).format(
                table_name=sql.Identifier(table_name),
            )
            curs.execute(stmt)
            result = curs.fetchone()

        rowcount, = result
        if rowcount == None:
            curs.close()
            conn.close()
            return
        return rowcount

    except psycopg2.OperationalError as e:
        print('SQL Exception !', e)
        sys.exit(1)
    finally:
        # closing database connection.
        if (conn):
            curs.close()
            conn.close()


def main():
    if len(sys.argv) != 2:
        print(" ")
        print("Usage: test.py  <data_dir>")
        print(" ")
        return

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.ERROR
    )
    data_dir = sys.argv[1]

    print("Total Database Row Count : ", query_table('kafka_data'))
    print("Total file Count : ", scan_data(data_dir))


if __name__ == "__main__":
    main()
