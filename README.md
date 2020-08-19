# The sources in this project demonstrate the ability to send data received GPS location from Bikes telematics, in realtime through kafka into the PostgreSQL database

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
    Every time you run this application it will drops the table recreate the table

e.g. #  python3.8 config.py

3) Start the Kafka cluster, if it’s not running


4) The step will stream the data from the files present in the directory  specified in directory "data_dir". As part of the demo I have some demo files containing data in this directory by the name "CSV".  When the program is run it will sort and stream all the files and send the data on the specified topic_name. if the topic is not there it will create  the topic programmatically. The program uses certificate based authentication using SSL. Please pass cert ,pem file and keys  as a part of the usage to connect with aiven kafka cloud.

Usage: streamtokafka <topic_name> <broker_ip:portno> <data_dir> <stream_freq> <ca> <cert> <key>
 
 topic_name	  : Name of the topic to stream data
 broker_ip:portno : IP Address or URL of the kafka broker along with portno. Provide Portno 
 data_dir         : directory where the streaming files are located
 stream_freq      : Streaming Frequency in seconds 
 ca               : CA file for certificate verification 
 cert             : Client Certificate file
 key              : Client Private key 

e.g. python3.8 streamtokafka.py gps-3  kafka-df561d5-sajeev-092f.aivencloud.com:15909 csv 3   keys/ca.pem keys/service.cert  keys/service.key 

5) The step will receive the streaming data as a bytes from the topic_name that was used in the step #4. The program decodes the bytes into the text and splits 
 the text based on  newline character and inserts the data into the table "kafka_data".The program uses certificate based authentication using SSL. Please pass cert ,pem file and keys  as a part of the usage to connect with aiven kafka cloud.


Usage: receive_from_kafka  <topic_name> <broker_ip:portno> <ca> <cert> <key>
 
 topic_name	  : Name of the topic to stream data
 broker_ip:portno : IP Address or URL of the kafka broker along with portno. Provide Portno 
 ca               : CA file for certificate verification 
 cert             : Client Certificate file
 key              : Client Private key 

e.g. python3.8 receive_from_kafka.py gps-3  kafka-df561d5-sajeev-092f.aivencloud.com:15909  keys/ca.pem keys/service.cert  keys/service.key

Testing:

1) The provided  script below will read total count from the aiven database. The count should be equivalent to the no of lines in the files present in <data_dir> directory.
   This ensures that total of lines that was streamed from the data directory is same is the one seen in the database
   e.g. #Usage: python3.8 test.py  

   Usage: test.py  <data_dir>

   python3.8 test.py csv

2) Match the contents of the database table data_dir and files that is created in data_dir 
   a) select * from defaultdb.data_dir; 
   b) cat csv/dummy.csv

3) If you need to create a new files that generate GPS data. Please use the below script. The script is facilitate with the ability to create a sample JSON data in the data_directory of your choice.

Usage: generate.py  <telematic_id> <file_name> <noof-gps-cordinates

e.g.   python3.8 generate.py  csv/dummy.txt 200 

Optimization:

     -- The script  receive_from_kafka.py reads the bytes from the topic_name and split them in lines, inserts one by one into the database as is. There should be ETL process 
        or  database trigger that takes the data from kafka_data and do a post processing  and inserts the data into reporting database for reports that is required for 
        the business use case.
        e.g. I would like to use the mileage of my bikes based on the telematic id,latitude and longitude information.

      
