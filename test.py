import os
import sys,logging
import subprocess
import socket
import pdb
import time
import psycopg2
from psycopg2 import Error
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


# function is used to query the table kafka_data. 
def query_table():
    param = config();
    conn = psycopg2.connect(**param)
    curs = conn.cursor()
    
    SQLONE = ''' select count(*) from kafka_data;'''
    
    try:
      curs.execute(SQLONE) 
      row = curs.fetchone()

      if row == None:
            curs.close()
            conn.close()
            return

      print( "Total Database Row Count : ",f"{row[0]}")
      print( "Please do wc -l | file_name. The count should match the database count  ")

    except Exception:
      print( "Exception  querying the data")
    finally:
    #closing database connection.
        if(conn):
            curs.close()
            conn.close()

def main():
    logging.basicConfig(
 	   format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
	    level=logging.INFO
    )
    query_table()



if __name__ == "__main__":
    main()


