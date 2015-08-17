CREATE EXTERNAL TABLE maint_table

(resourceid STRING, eventDate STRING,
technician STRING, description STRING)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ","


STORED AS TEXTFILE LOCATION "/user/user01/sensormaint.csv";
