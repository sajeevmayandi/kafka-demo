# The sources in this project demonstrate the ability to send data through Kafka into the PostgreSQL database.


# Pre-Requisites
- Python version > 3.0

- install kafka-ptyhon   e.g python3.8 pip install kafka-python
  #It’s a client that is been used in the sources to connect to the Kafka, primarily to publish and subscribe the message.  
     More info https://pypi.org/project/kafka-python/

- install psycopg2
  #its PostgreSQL adapter for python programming.

# Usage and steps

1) Edit database.ini in the source and store your database credentials under the PostgreSQL section.

[postgresql]
host=<host name or URI >      # hostname or URL where of postgres database
database=<database name>      # database name in the RDS instance where the data will be stored
user=<username>               # username of the database
password=<password>           # password of the database


2)  This step configures the database with the database table. The default database in aiven is defaultdb .  After the successful completion of the step 
    database will have  table by the name "kafka_data" created in the public  schema.Streaming data gets dumped into this table as TEXT in column "msg". 
    Every time you run this application it will delete the table recreate the table

e.g. #  python3.8 config.py

3) Start the Kafka cluster, if it’s not running


4) The step will stream the data from the files present in the directory  specified in directory "data_dir". As part of the demo I have some demo files containing data in this directory by the name "CSV".  When the program is run it will sort and stream all the files and send the data on the specified topic_name. if the topic is not there it will create  the topic programmatically. The program uses certificate based authentication using SSL. I have cert and pem file stored in the keys directory the program picks up those keys to connect with aiven kafka cloud.

   Usage: streamtokafka <topic_name> <broker_ip> <data_dir> <stream_frequency_seconds>
   e.g. # python3.8 streamtokafka.py demo kafka-df561d5-sajeev-092f.aivencloud.com:15909 csv 30

   topic_name              : The topic name where you want to stream the data
   broker_ip               : The IP address of the broker with the port no e.g. kafka-df561d5-sajeev-092f.aivencloud.com:15909
   data_dir                : The directory where the files with the data is stored
   stream_frequency_seconds: the frequency at which the data is streamed to the kafka

5) The step will receive the streaming data as a bytes from the topic_name that was used in the step #4. The program decodes the bytes into the text and splits 
 the text based on  newline character and inserts the data into the table "kafka_data".The program uses certificate based authentication using SSL. I have cert and pem file stored in the keys directory the program picks up those keys to connect with aiven kafka cloud.

   e.g. #Usage: python3.8 receive_from_kafka  demo  demo kafka-df561d5-sajeev-092f.aivencloud.com:15909

6) The step allows you verify the result. the script below will read total count from the aiven database. The count should be equivalent to the no of lines in the files present in csv directory
   e.g. #Usage: python3.8 test.py  

  # Optimization:
     -- database password could be send as parameter to the config.py and receive_from_kafka.py. This ensures that database password is not compromised if anybody got the
        access to the system.
     -- The script  receive_from_kafka.py reads the bytes from the topic_name and split them in lines, inserts one by one into the database as is. There should be ETL process 
        or  database trigger that takes the data from kafka_data and do a post processing  and inserts the data into reporting database for reports that is required for 
        the business use case.
        e.g. I would like to use the mileage of my bikes based on the mac address and latitude and longitude information.

     -- More connection pooling can be done while inserting data into the database. e.g. right now for every write a database connection is opened, inserted and committed and
        connection is closed . Have a pool of connection opened and re-use them for every insert transaction.

      -- test.py can count the no of lines in the data directory and match that with total count inserted in the database
      
