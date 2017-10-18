package com.hashmap.bechtel.app.sap.batch

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.json.{Json, Writes}

import scala.collection.mutable.ListBuffer

object BulkLoadTables extends App
   // with SparkApplicationContext
{

  val conf = new SparkConf().setAppName("bbs.becpsn.com.BulkLoadTables")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  val sqlContext = SQLContext.getOrCreate(sc)
  println("args:" + args.length)
  if (args.length != 4 && args.length != 5) {
    println("This Job accepts 4 parameters <jobName> <sourceFileDir> <partitions> <dbName> <ingestion_summary_topic> <dead_letter_topic>")
    println("OR")
    println("This Job accepts 5 parameters <jobName> <sourceFileDir> <partitions> <dbName> <ingestion_summary_topic> <dead_letter_topic> local")

    System.exit(1)
  }

  val jobName: String = args(0)
  val source: String = args(1)
  var minPartition = args(2)
  var dbName = ""

  val dbNameOps = "bbs_sap_ops"

  if (args.length == 5) {
    if ("local".equals(args(4))) {
      //      conf.setMaster(args(4))
      dbName = args(3) + "."
    }
  } else if (args.length == 4) {
    if ("local".equals(args(3))) {
      //      conf.setMaster(args(3))
    } else {
      dbName = args(3) + "."
    }
  }

  val broker = "10.182.69.0:6667"
  val zkUrl = "10.182.69.0:2181"
  def kafkaProducerProperties(kafkaBroker: String,zookeeperUrl: String,id: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBroker)
    //    props.put("zk.connect", zookeeperUrl)
    props.put("client.id", id)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  private lazy val tableInsertSummaryProducer = new KafkaProducer[String, String](kafkaProducerProperties(broker,zkUrl,"tableInsertSummaryProducer"))
  private lazy val deadLetterProducer = new KafkaProducer[String, String](kafkaProducerProperties(broker,zkUrl,"deadLetterProducer"))
  import org.apache.hadoop.fs.{FileSystem, Path}
  val files = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(source))

  import org.apache.commons.io.FilenameUtils

  case class TableIngestSummary (schema: String,tablename: String, filename: String, opr_ind:String, inserttype: String,noofrecords: Long,  recordsmessage: String, sap_genTime: String,  inserttime: Long)
  implicit val subWrites:Writes[TableIngestSummary] = Json.writes[TableIngestSummary]

  case class TableIngestError (hive_schema: String, rows: String,table: String)
  implicit val subErrWrites:Writes[TableIngestError] = Json.writes[TableIngestError]


  val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  def processFile(file: String) = {
    val hive_schema = "bbs_sap_pr"
    val insertTime = System.currentTimeMillis()
    //val genTime = filename.split("-")(1).mkString.replace(".json","").replace("UPD","").replace("INS","").replace("DEL","")
    val filename = FilenameUtils.getBaseName(file)
    val fnregexpattern = """(\w+)-([A-Za-z]+)([0-9]+)""".r
    val fnregexpattern(table,opr_ind,genTime) = filename

    try {
      //println("file_name:" + file)

      //val genTime = filename.split("-")(1).mkString.replace(".json","").replace("UPD","").replace("INS","").replace("DEL","")
      val generationTime = genTime match{
        case "" => System.currentTimeMillis()
        case _ => dateFormat.parse(genTime).getTime
      }
      //val opr_ind = filename.split("-")(1).substring(0,3)
      //println("tableName:" + table)
      val df = sqlContext.read.json(file)

      import org.apache.spark.sql.functions._
      //      import sqlContext.implicits._
      //      val currentTime = (() => System.currentTimeMillis().toString)
      import org.apache.spark.sql.functions

      if(!df.take(1).isEmpty) {

        if (df.columns.contains("_corrupt_record")) {

          val df1 = df.filter("`_corrupt_record` is null").drop(df.col("_corrupt_record"))
          if(!df1.take(1).isEmpty) {
            val df2 = df1.withColumn("inserttime", functions.lit(insertTime))
            df2.printSchema()
            df2.write.mode("append").format("orc").saveAsTable(dbName + table)
            val timeToInsert = filename.split("-").take(1).mkString.replace(".json","").replace("UPD","").replace("INS","").replace("DEL","")

            val tableIngestSummary = TableIngestSummary(dbName.replace(".",""), table, file,opr_ind, "batch", df2.count(), "", genTime, insertTime)
            val msg = play.api.libs.json.Json.toJson(tableIngestSummary).toString()
            val dataRecord = new ProducerRecord[String, String]("ingestion_summary_pr", null, msg)
            tableInsertSummaryProducer.send(dataRecord)
          }
          val df3 = df.selectExpr("`_corrupt_record`").filter("`_corrupt_record` is not null").withColumn("table", functions.lit(dbName + table)).withColumn("inserttime", functions.lit(insertTime))
          //val df4 = df3.withColumn("inserttime", functions.lit(System.currentTimeMillis().toString))
          df3.write.mode("append").format("orc").saveAsTable(dbNameOps + ".corrupt_records")

        } else {
          val df1 = df.withColumn("inserttime", functions.lit(generationTime))
          df1.printSchema()
          df1.write.mode("append").format("orc").saveAsTable(dbName + table)
          val tableIngestSummary = TableIngestSummary(dbName.replace(".",""), table, filename, opr_ind, "batch", df1.count(), "", genTime, insertTime)

          val msg = play.api.libs.json.Json.toJson(tableIngestSummary).toString()
          val dataRecord = new ProducerRecord[String, String]("ingestion_summary_pr", null, msg)
          tableInsertSummaryProducer.send(dataRecord)
        }
      }
    } catch {
      case ex => {
        val tableIngestSummary = TableIngestSummary(dbName.replace(".",""), "", "", "batch","",0, "Failed processesing " + ex.toString(), genTime, insertTime)
        val msg = play.api.libs.json.Json.toJson(tableIngestSummary).toString()
        val errored = new ProducerRecord[String, String]("dead_letter_pr", tableIngestSummary.tablename, msg)
        deadLetterProducer.send(errored)
        println("Processing Failed: " + file)
        ex.printStackTrace()
      }
    }


  }

  files.foreach(filename => {
    // the following code makes sure "_SUCCESS" file name is not processed
    val a = filename.getPath.toString()

    println("\nFILENAME: " + a)
    processFile(a)


  })
  tableInsertSummaryProducer.close()
  deadLetterProducer.close()
}
