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

"""
The purpose of this tutorial is to connect to create tables for the PostgreSQL database server"

Step 1: CREATE TABLE table_name
Step 2: process creatin in Python
Step 3: Connect to DB if not connected

"""
def create_tables():
    """ Create tables in the PostgreSQL database """
    commands = (
        """
        CREATE TABLE vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
        """,
        """
        CREATE TABLE parts (
            part_id SERIAL PRIMARY KEY,
            part_name VARCHAR(255) NOT NULL
        )

        """,
        """
        CREATE TABLE part_drawings (
            part_id INTEGER PRIMARY KEY,
            file_extension VARCHAR(5) NOT NULL,
            drawing_data BYTEA NOT NULL,
            FOREIGN KEY (part_id)
                REFERENCES parts (part_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE vendor_parts (
            vendor_id INTEGER NOT NULL,
            part_id INTEGER NOT NULL,
            PRIMARY KEY (vendor_id, part_id),
            FOREIGN KEY (vendor_id)
                REFERENCES vendors (vendor_id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (part_id)
                REFERENCES parts (part_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """
    )
    conn = None
    try:
        # read the connection parameters
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # creatae table one by None
        for command in commands:
            cur.execute(command)
        # close communication with PostgreSQL database server
        cur.close()
        #commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

"""
The purpose of this tutorial is to insert data into the vendors table of our Database
We'll be inserting a single entry (record) as well as a list containing multiple records

Step 1: Connect to DB if not connected
"""

def insert_vendor(vendor_name):
    """ insert a new vendor into the vendor table """
    sql = """INSERT INTO vendors(vendor_name) VALUES(%s) RETURNING vendor_id;"""
    conn = None
    vendor_id = None
    try:
        # read database configration
        params = config()
        conn = psycopg2.connect(**params)
        # create new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (vendor_name,))
        # get the generated id back
        vendor_id = cur.fetchone()[0]
        # commit changes to the Database
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return vendor_id

def insert_vendor_list(vendor_list):
    sql = "INSERT INTO vendors(vendor_name) VALUES(%s)"
    conn =  None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute SQL INSERT statement
        cur.executemany(sql, vendor_list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    # insert one vendor
    insert_vendor("Selamta Honey")
    insert_vendor_list([
        ('Makeda Inc.',),
        ('Yesus Yadinal Ltd.',),
        ('Dalhak Industries Ltd.',),
    ])
