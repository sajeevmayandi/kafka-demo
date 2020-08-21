import psycopg2
from psycopg2 import sql
from psycopg2 import Error
from configparser import ConfigParser
import sys


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


# function creates the initial schema
def create_schema(schema_file: str):
    conn = None
    try:
        param = config();
        conn = psycopg2.connect(**param)
        curs = conn.cursor()
        curs.execute(open(schema_file, "r").read())
        conn.commit()
        print("Schema created successfully in PostgreSQL ")
    except Exception:
        print("Exception  Schema creation ")
        conn.rollback()
        sys.exit(1)
    finally:
        # closing database connection.
        if (conn):
            curs.close()
            conn.close()


def main():
    if len(sys.argv) != 2:
        print(" ")
        print("Usage: config  <schema_file>")
        print(" ")
        return
    file_name = sys.argv[1]

    create_schema(file_name)


if __name__ == "__main__":
    main()
