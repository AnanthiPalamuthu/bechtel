package com.hashmap.bechtel.app.sap.streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.hive._

import scala.collection.mutable.{ListBuffer, Map}

object SAPStream extends App
  //  with SparkApplicationContext
  with KafkaConfigurations {
  lazy val streamingContext = new StreamingContext(conf, Seconds(3))
  private lazy val log = Logger.getLogger(getClass)
  val conf = new SparkConf().setAppName("StreamingSparkApp")
  val ssc = new StreamingContext(conf, Seconds(2))
  val sc = SparkContext.getOrCreate(conf)
  val hiveContext = SAPStream.getHiveInstance(sc)
  val sapQueue = Set(args(0))

  if (args.length != 3) {
    log.error("Job accepts 3 parameters <ProcesseDataTopic> <Broker> <zkkafka>")
  }
  val broker = args(1)
  val zkUrl = args(2)
  val kafkaParams = Map[scala.Predef.String, java.lang.Object](
    "bootstrap.servers" -> broker,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "sapQueue",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val dataStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConsumerProperties(broker, zkUrl), sapQueue)
  log.info("****************create stream****************")
  @transient private var hiveInstance: HiveContext = null

  def processMsg(sc: SparkContext, key: String, msg: String): Unit = {
    val hiveContext = SAPStream.getHiveInstance(sc)
    val table = key
    println("table name:" + table)
    println(msg)
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StringType, StructField}

    var columnNames: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
    var rows = ListBuffer[Row]()
    import com.fasterxml.jackson.core.`type`.TypeReference
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.DefaultScalaModule

    import scala.collection.JavaConversions._
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val typeRef: TypeReference[java.util.List[java.util.HashMap[java.lang.String, Object]]] = new TypeReference[java.util.List[java.util.HashMap[java.lang.String, Object]]]() {}
    val recs = mapper.readValue[java.util.List[java.util.HashMap[java.lang.String, Object]]](msg, typeRef)
    var collectNames: Boolean = true
    recs.toList.foreach(rec => {
      if (!columnNames.isEmpty) collectNames = false

      var columnValues: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      rec.foreach(kv => {
        if (collectNames) columnNames += kv._1.mkString
        columnValues += kv._2.toString
        //            println(kv._1 + " = " + kv._2)
      })
      val rowschema = org.apache.spark.sql.types.StructType(
        columnNames.map(fieldName => StructField(fieldName, StringType, true)))
      import org.apache.spark.sql.catalyst.expressions._
      val row = new GenericRowWithSchema(columnValues.toArray, rowschema)

      rows += row

      println("===============")
    })
    import collection.JavaConverters._
    val rowsJ = rows.asJava
    val schema = org.apache.spark.sql.types.StructType(
      columnNames.map(fieldName => StructField(fieldName, StringType, true)))
    import hiveContext.implicits._
    val df = hiveContext.createDataFrame(rowsJ, schema)
    import org.apache.spark.sql.functions._
    val df1 = df.withColumn("timestamp",current_timestamp())
    println(table + " has schema: " + df1.printSchema())
    println(table + " records: ")
    println(df1.show)
    df1.write.mode("append").format("orc")saveAsTable(table )

  }

  dataStream.foreachRDD { rdd => {
    //    rdd.foreachPartition { iter =>

    import hiveContext.implicits._
    rdd.collect.foreach { case (key, msg) => processMsg(rdd.sparkContext, key, msg)

    }
    //    }
  }
  }

  def getHiveInstance(sparkContext: SparkContext): HiveContext = synchronized {
    if (hiveInstance == null) {
      hiveInstance = new HiveContext(sparkContext)
    }
    hiveInstance
  }

  ssc.start()
  ssc.awaitTermination()
  log.info("****************done****************")

}
