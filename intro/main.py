from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timedelta
import random
import numpy as np


def main():
    """The main routine."""
    session = getDBSession()
    generateDataEntrypoint(session)


def getDBSession():
    """Create and get a Cassandra session"""
    cloud_config= {
            'secure_connect_bundle': '<path_to_zip_downloaded_from_setup_step>'
    }
    auth_provider = PlainTextAuthProvider('<client_id_from_setup_step>', '<client_secret_from_setup_step>')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    return session


def generateInsertData(t_id, number_of_rows, session):
    """A function which completes generation of data and insertion of data to Cassandra"""
    readings = [i for i in np.arange(0,100,0.05)]
    device_ids = [i for i in np.arange(1,1000,1)]
    capture_start = datetime(2017, 12, 10, 00, 00, 0)

    insert_query = session.prepare("\
                INSERT INTO cassandra_pythondemo.demo_readings (device_id, timeseries_id, value_ts, publication_ts, value)\
                VALUES (?, ?, ?, ?, ?)\
                IF NOT EXISTS\
                ")

    device_id = random.choice(device_ids)
    for t in range(0, number_of_rows):
        t_pub_ts = datetime.now()
        value_ts = capture_start + timedelta(minutes=t)
        
        try:
            session.execute(insert_query, [device_id, t_id, value_ts, t_pub_ts, round(random.choice(readings),2)])
            print("Success")
        except Exception as e: 
            print(e)


def generateDataEntrypoint(session):
    """Main driver for generating data"""
    t_ids = [i for i in np.arange(100000,200000,1)]
    start = datetime.now()
    timeseries_to_generate = 2
    number_of_rows = 10
    i = 0

    while i < timeseries_to_generate:
        t_id = random.choice(t_ids)
        print("Processing:"+str(t_id))
        generateInsertData(t_id, number_of_rows, session)
        i += 1

    end = datetime.now()
    print(start)
    print(end)

if __name__ == "__main__":
    main()