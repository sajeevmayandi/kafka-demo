import psycopg2
from psycopg2 import sql
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
def create_table(table_name: str):
    conn = None
    try:
        param = config();
        conn = psycopg2.connect(**param)
        curs = conn.cursor()

        with curs as cursor:
            stmt = sql.SQL("""
            DROP TABLE IF EXISTS {table_name};
             CREATE TABLE {table_name} (
               msg text,
                created_on timestamp with time zone DEFAULT now()
              );
              ALTER TABLE {table_name}  OWNER to avnadmin;
            """).format(
                table_name=sql.Identifier(table_name), )

            curs.execute(stmt)
        print("Table created successfully in PostgreSQL ")
    except Exception:
        print("Exception  Database creation ")
        conn.rollback()
    finally:
        # closing database connection.
        if (conn):
            curs.close()
            conn.close()


def main():
    create_table("kafka_data")


if __name__ == "__main__":
    main()
