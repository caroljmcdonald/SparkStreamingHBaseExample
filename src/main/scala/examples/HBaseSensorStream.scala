/*
 * This example reads a row of time series sensor data
 * calculates the the statistics for the hz data 
 * and then writes these statistics to the stats column family
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
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path

object HBaseSensorStream {
  final val tableName = "/user/user01/sensor"
  final val cfData = "data"
  final val cfDataBytes = Bytes.toBytes(cfData)
  final val cfAlert = "alert"
  final val cfAlertBytes = Bytes.toBytes(cfAlert)
  
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)

  object Sensor {
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
      put.add(cfDataBytes, Bytes.toBytes("hz"), Bytes.toBytes(sensor.hz))
      put.add(cfDataBytes, Bytes.toBytes("disp"), Bytes.toBytes(sensor.disp))
      put.add(cfDataBytes, Bytes.toBytes("flo"), Bytes.toBytes(sensor.flo))
      put.add(cfDataBytes, Bytes.toBytes("sedPPM"), Bytes.toBytes(sensor.sedPPM))
      put.add(cfDataBytes, Bytes.toBytes("psi"), Bytes.toBytes(sensor.psi))
      put.add(cfDataBytes, Bytes.toBytes("chlPPM"), Bytes.toBytes(sensor.chlPPM))
      (new ImmutableBytesWritable, put)
    }
    // convert psi alert to an HBase put object
    def convertToPutAlert(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val key = sensor.resid + "_" + dateTime
      val p = new Put(Bytes.toBytes(key))
      // add to column family alert, column psi data value to put object 
      p.add(cfAlertBytes, Bytes.toBytes("psi"), Bytes.toBytes(sensor.psi))
      (new ImmutableBytesWritable, p)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseStream")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val conf = HBaseConfiguration.create()
    // set HBase table to write to
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    // set directory to read streaming data from
    val lines = ssc.textFileStream("/user/user01/stream")
    lines.print()
    lines.foreachRDD { rdd =>
      // parse the line of data into sensor object
      val sensorRDD = rdd.map(Sensor.parseSensor)
      sensorRDD.take(1).foreach(println)
      
      // filter sensor data for low psi
      val alertRDD = sensorRDD.filter(sensor => sensor.psi < 5.0)
      alertRDD.take(1).foreach(println)

      // set JobConfiguration variables for writing to HBase
      val jobConfig: JobConf = new JobConf(conf, this.getClass)
      jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
      jobConfig.setOutputFormat(classOf[TableOutputFormat])
      jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      // convert sensor data to put object and write to HBase table column family data
      new PairRDDFunctions(sensorRDD.map(Sensor.convertToPut)).saveAsHadoopDataset(jobConfig)
      // convert alert data to put object and write to HBase table column family alert
      new PairRDDFunctions(alertRDD.map(Sensor.convertToPutAlert)).saveAsHadoopDataset(jobConfig)
    }
    ssc.start()
    ssc.awaitTermination()

  }

}