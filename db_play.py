import psycopg2
from config import config

"""
The purpose of this tutorial is to connect to a PostgreSQL database server that we created.

Step 1: CREATE DATABASE suppliers
Step 2: Create a database.ini that holds the connection parameters to our DB
Step 3: Create a config.py file to prase the DB connection parameters on database.ini
Step 4: Connect to the DB

"""

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version: ')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed!')

if __name__ == '__main__':
    connect()
