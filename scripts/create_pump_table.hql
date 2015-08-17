CREATE EXTERNAL TABLE pump_info

(resourceid STRING, type STRING, purchasedate STRING,
dateinservice STRING, vendor STRING, longitude FLOAT, latitude FLOAT)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ","


STORED AS TEXTFILE LOCATION "/user/user01/sensorvendor.csv";
