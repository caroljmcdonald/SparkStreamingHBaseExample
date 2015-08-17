
Create an hbase table to write to:
launch the hbase shell
$hbase shell

create '/user/user01/sensor', {NAME=>'data'}, {NAME=>'alert'}, {NAME=>'stats'}

Commands to run labs:

Step 1: First compile the project: Select project  -> Run As -> Maven Install

Step 2: use scp to copy the sparkstreamhbaseapp-1.0.jar to the mapr sandbox or cluster

To run the  streaming:

Step 3: start the streaming app

/opt/mapr/spark/spark-1.3.1/bin/spark-submit --class examples.HBaseSensorStream sparkstreamhbaseapp-1.0.jar

Step 4: copy the streaming data file to the stream directory
cp sensordata.csv  /user/user01/stream/.

Step 5: you can scan the data written to the table, however the values in binary double are not readable from the shell
launch the hbase shell,  scan the data column family and the alert column family 
$hbase shell
scan '/user/user01/sensor',  {COLUMNS=>['data']}
scan '/user/user01/sensor',  {COLUMNS=>['alert']}

Step 6: launch one of the programs below to read data and calculate daily statistics
calculate stats for one column
/opt/mapr/spark/spark-1.3.1/bin/spark-submit --class examples.HBaseReadWrite sparkstreamhbaseapp-1.0.jar
calculate stats for whole row
/opt/mapr/spark/spark-1.3.1/bin/spark-submit --class examples.HBaseReadRowWriteStats sparkstreamhbaseapp-1.0.jar

launch the shell and scan for statistics
scan '/user/user01/sensor',  {COLUMNS=>['stats']}


