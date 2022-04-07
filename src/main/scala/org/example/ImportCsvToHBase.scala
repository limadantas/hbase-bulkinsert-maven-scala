package org.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.api.java._
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.util

class ImportCsvToHBase {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ImportCsvToHBase])

  @throws[Exception]
  def process(csvPath: String, zkQuorum: String, zkPort: String, tableName: String, rowKey: String, columnFamily: String): Unit = {
    val appName: String = "ImportCsvToHBase"

    val config: InputParams = new InputParams

    config.setInputFile(csvPath)
    config.setQuorum(zkQuorum)
    config.setPort(zkPort)
    config.setTableName(tableName)
    config.setRowKey(rowKey)

    // Spark Config
    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster("local")
    val sc: JavaSparkContext = new JavaSparkContext(conf)
    val sparkSession: SparkSession = SparkSession.builder.appName(appName).config(conf).getOrCreate
    // create connection with HBase
    var configuration: Configuration = null
    try {
      configuration = HBaseConfiguration.create
      configuration.set("hbase.zookeeper.quorum", config.getQuorum)
      configuration.set("hbase.zookeeper.property.clientPort", config.getPort)
      HBaseAdmin.checkHBaseAvailable(configuration)
      LOG.info("------------------HBase is running!------------------")
    } catch {
      case ce: Exception =>
        ce.printStackTrace()
    }
    // new Hadoop API configuration
    val newAPIJobConfiguration: Job = Job.getInstance(configuration)
    newAPIJobConfiguration.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, config.getTableName)
    newAPIJobConfiguration.setOutputFormatClass(classOf[TableOutputFormat[_]])
    val ROW_KEY_B: Broadcast[String] = sc.broadcast(config.getRowKey)

    val inputDF = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").csv(config.getInputFile)
    inputDF.printSchema()
    val columns = inputDF.columns.toSeq
    val rowValues = new util.ArrayList[util.HashMap[String, String]]

    columns.foreach(col => {
      if (col != config.getRowKey) {
        val field = new util.HashMap[String, String]()
        field.put("qualifier", columnFamily + ':' + col)
        field.put("value", col)
        rowValues.add(field)
      }
    })
    config.setRowValues(rowValues)

    val ROW_VALUES_B: Broadcast[util.ArrayList[util.HashMap[String, String]]] = sc.broadcast(config.getRowValues)

    val hbasePuts: JavaPairRDD[ImmutableBytesWritable, Put] = inputDF.javaRDD.mapToPair(new PairFunction[Row, ImmutableBytesWritable, Put]() {
      @throws[Exception]
      override def call(data: Row): (ImmutableBytesWritable, Put) = {
        val royKeys: Array[String] = ROW_KEY_B.value.split(":")
        var key: String = ""
        for (k <- royKeys) {
          key = key + data.getAs(k) + ":"
        }
        key = key.substring(0, key.length - 1)
        val put: Put = new Put(Bytes.toBytes(key))
        import scala.collection.JavaConversions._
        for (v <- ROW_VALUES_B.value) {
          val cq: Array[String] = v.get("qualifier").split(":")
          put.addColumn(Bytes.toBytes(cq(0)), Bytes.toBytes(cq(1)), Bytes.toBytes(data.getAs(v.get("value")).toString))
        }
        new Tuple2[ImmutableBytesWritable, Put](new ImmutableBytesWritable, put)
      }
    }).cache
    val counter: Long = hbasePuts.count
    LOG.info("<-----------Total number of rows to be inserted--------->" + counter)
    hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration)
    sc.stop()
  }
}

class InputParams {
  private var inputFile :String = null
  private var quorum :String = null
  private var port :String = null
  private var tableName :String = null
  private var rowKey :String = null
  private var rowValues :util.ArrayList[util.HashMap[String, String]] = new util.ArrayList[util.HashMap[String, String]]

  def getInputFile: String = inputFile

  def setInputFile(inputFile: String): Unit = {
    this.inputFile = inputFile
  }

  def getQuorum: String = quorum

  def setQuorum(quorum: String): Unit = {
    this.quorum = quorum
  }

  def getPort: String = port

  def setPort(port: String): Unit = {
    this.port = port
  }

  def getTableName: String = tableName

  def setTableName(tableName: String): Unit = {
    this.tableName = tableName
  }

  def getRowKey: String = rowKey

  def setRowKey(rowKey: String): Unit = {
    this.rowKey = rowKey
  }

  def getRowValues: util.ArrayList[util.HashMap[String, String]] = rowValues

  def setRowValues(rowValues: util.ArrayList[util.HashMap[String, String]]): Unit = {
    this.rowValues = rowValues
  }
}