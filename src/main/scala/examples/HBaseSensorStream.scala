/*
 * 
 *  
 */

package examples

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object HBaseSensorStream {
  final val tableName = "/user/user01/sensor"
  final val cfDataBytes = Bytes.toBytes("data")
  final val cfAlertBytes = Bytes.toBytes("alert")
  final val colHzBytes = Bytes.toBytes("hz")
  final val colDispBytes = Bytes.toBytes("disp")
  final val colFloBytes = Bytes.toBytes("flo")
  final val colSedBytes = Bytes.toBytes("sedPPM")
  final val colPsiBytes = Bytes.toBytes("psi")
  final val colChlBytes = Bytes.toBytes("chlPPM")

  // schema for sensor data   
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)

  object Sensor {
    // function to parse line of sensor data into Sensor class
    def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
    }
    //  Convert a row of sensor object data to an HBase put object
    def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val rowkey = sensor.resid + "_" + dateTime
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object 
      put.add(cfDataBytes, colHzBytes, Bytes.toBytes(sensor.hz))
      put.add(cfDataBytes, colDispBytes, Bytes.toBytes(sensor.disp))
      put.add(cfDataBytes, colFloBytes, Bytes.toBytes(sensor.flo))
      put.add(cfDataBytes, colSedBytes, Bytes.toBytes(sensor.sedPPM))
      put.add(cfDataBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      put.add(cfDataBytes, colChlBytes, Bytes.toBytes(sensor.chlPPM))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }
    // convert psi alert to an HBase put object
    def convertToPutAlert(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val key = sensor.resid + "_" + dateTime
      val p = new Put(Bytes.toBytes(key))
      // add to column family alert, column psi data value to put object 
      p.add(cfAlertBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      return (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
    }
  }

  def main(args: Array[String]): Unit = {
    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val sparkConf = new SparkConf().setAppName("HBaseStream")
    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // parse the lines of data into sensor objects
    val sensorDStream = ssc.textFileStream("/user/user01/stream").map(Sensor.parseSensor)
    sensorDStream.print()

    sensorDStream.foreachRDD { rdd =>
      // filter sensor data for low psi
      val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
      alertRDD.take(1).foreach(println)
      // convert sensor data to put object and write to HBase table column family data
      rdd.map(Sensor.convertToPut).
        saveAsHadoopDataset(jobConfig)
      // convert alert data to put object and write to HBase table column family alert
      rdd.map(Sensor.convertToPutAlert).
        saveAsHadoopDataset(jobConfig)
    }
    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}