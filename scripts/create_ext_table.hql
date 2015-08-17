CREATE EXTERNAL TABLE sensor
        (key STRING, resID STRING, date STRING,
        hz FLOAT,
        disp FLOAT,
        flo INT,
        sedPPM FLOAT,
        psi INT,
        chlPPM FLOAT)
        STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES (
        "hbase.columns.mapping" =
        ":key,cf1:resID,cf1:date,cf1:hz,cf1:disp,
        cf1:flo,cf1:sedPPM,cf1:psi,cf1:chlPPM"
        )

TBLPROPERTIES("hbase.table.name" = "/user/user01/sensor");
