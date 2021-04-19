--DROP TABLE cassandra_pythondemo.demo_readings ;

CREATE TABLE cassandra_pythondemo.demo_readings (
     device_id int,
     timeseries_id int,
     value_ts timestamp,
     publication_ts timestamp,
     value double,
     PRIMARY KEY (device_id, timeseries_id, value_ts)
);