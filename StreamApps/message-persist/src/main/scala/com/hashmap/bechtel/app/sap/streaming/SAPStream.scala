package com.hashmap.bechtel.app.sap.streaming

import java.text.SimpleDateFormat

import com.hashmap.bechtel.app.sap.streaming.SAPStream.TableIngestError
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
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
import play.api.libs.json.{Json, Writes}

import scala.collection.mutable.{ListBuffer, Map}

object SAPStream extends App
  //  with SparkApplicationContext
  with KafkaConfigurations {
  lazy val streamingContext = new StreamingContext(conf, Seconds(3))
  private lazy val log = Logger.getLogger(getClass)
  val conf = new SparkConf().setAppName("bbs.becpsn.com.SAPStreamHR")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
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
  private lazy val tableInsertSummaryProducer = new KafkaProducer[String, String](kafkaProducerProperties(broker,zkUrl,"tableInsertSummaryProducer"))
  private lazy val deadLetterProducer = new KafkaProducer[String, String](kafkaProducerProperties(broker,zkUrl,"deadLetterProducer"))

  val numInputDStreams = 5
  //val kafkaDStreams = (1 to numInputDStreams).map { _ => val ds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConsumerProperties(broker, zkUrl), sapQueue)}
  val dataStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConsumerProperties(broker, zkUrl), sapQueue)
  log.info("****************create stream****************")
  @transient private var hiveInstance: HiveContext = null
  case class TableIngestSummary (schema: String,tablename: String, filename: String, opr_ind:String, inserttype: String,noofrecords: Long,  recordsmessage: String, sap_genTime: String,  inserttime: Long)
  implicit val subWrites:Writes[TableIngestSummary] = Json.writes[TableIngestSummary]

  case class TableIngestError (hive_schema: String, rows: String,table: String)
  implicit val subErrWrites:Writes[TableIngestError] = Json.writes[TableIngestError]
  val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  def processMsg(sc: SparkContext, key: String, msg: String): Unit = {
    val hiveContext = SAPStream.getHiveInstance(sc)
    val hive_schema = "bbs_sap_pr."
    val keypattern="""(\w+) ([0-9]+)""".r
    val keypattern(table, genTime) = key
    //val genTime = key.split(" ").lift(1).mkString
    //println(s"table=$table genTime=$genTime message=$msg")
    //val generationTime = genTime match{
    //  case "" => System.currentTimeMillis()
    //  case _ => dateFormat.parse(genTime).getTime
    //}

    val insertTime = System.currentTimeMillis()
    //val table = hive_schema + key.split(" ").lift(0).mkString
//    println("table name:" + table)
//    println(msg)
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
    var opr_ind = "INS"
    recs.toList.foreach(rec => {
      if (!columnNames.isEmpty) collectNames = false

      var columnValues: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer()
      rec.toSeq.sortBy(_._1).foreach(kv => {
        if (collectNames){
          columnNames += kv._1.mkString
          if("opr_ind".equalsIgnoreCase(kv._1.mkString)) opr_ind = kv._2.toString
        }

        columnValues += kv._2.toString
        //            println(kv._1 + " = " + kv._2)
      })



      val rowschema = org.apache.spark.sql.types.StructType(
        columnNames.map(fieldName => StructField(fieldName, StringType, true)))
      import org.apache.spark.sql.catalyst.expressions._
      val row = new GenericRowWithSchema(columnValues.toArray, rowschema)

      rows += row

//      println("===============")
    })
    try {
      import collection.JavaConverters._
      val rowsJ = rows.asJava
      val schema = org.apache.spark.sql.types.StructType(
        columnNames.map(fieldName => StructField(fieldName, StringType, true)))
      import hiveContext.implicits._
      val df = hiveContext.createDataFrame(rowsJ, schema)
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.functions

      val df1 = df.withColumn("inserttime", functions.lit(insertTime))
      println(table + " has schema: " + df1.printSchema())
      //    println(table + " records: ")
      println(df1.show)

      df1.write.mode("append").format("orc") saveAsTable (hive_schema + table)
      val tableIngestSummary = TableIngestSummary(hive_schema, key.split(" ").lift(0).mkString, "",opr_ind , "stream",df1.count(), "", genTime,insertTime)
      val msg = play.api.libs.json.Json.toJson(tableIngestSummary).toString()
      val dataRecord = new ProducerRecord[String, String]("ingestion_summary_pr", null, msg)
      tableInsertSummaryProducer.send(dataRecord)
    } catch {
      case e => {
        println("Processing Failed table: " + key + " " + msg)
        val tableIngestError = TableIngestError(hive_schema, msg ,key.split(" ").lift(0).mkString)
        val msgp = play.api.libs.json.Json.toJson(tableIngestError).toString()
        val errored = new ProducerRecord[String, String]("dead_letter_pr", tableIngestError.table, msgp)
        deadLetterProducer.send(errored)
        e.printStackTrace()
      }
    }
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
//  sys.ShutdownHookThread {
//    log.info("Gracefully stopping Spark Streaming Application")
//    ssc.stop(true, true)
//    log.info("Application stopped")
//  }
  ssc.start()
  ssc.awaitTermination()
  tableInsertSummaryProducer.close()
  deadLetterProducer.close()
  log.info("****************done****************")

}
