import psycopg2
from psycopg2 import Error
from configparser import ConfigParser

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

# function creates the initiall schema
def create_table():
    param = config();
    conn = psycopg2.connect(**param)
    curs = conn.cursor()
    
    SQL = ''' DROP TABLE IF EXISTS kafka_data;
              CREATE TABLE kafka_data (
                msg text,
                created_on timestamp with time zone DEFAULT now()
              );
              ALTER TABLE kafka_data OWNER to postgres; '''
    
    try:
      curs.execute(SQL) 
      conn.commit()
      print("Table created successfully in PostgreSQL ")
    except Exception:
      print( "Exception  inserting the data")
      conn.rollback()
    finally:
    #closing database connection.
        if(conn):
            curs.close()
            conn.close()

def main():
    create_table()



if __name__ == "__main__":
    main()


