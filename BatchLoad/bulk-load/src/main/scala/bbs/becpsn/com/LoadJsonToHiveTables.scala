package bbs.becpsn.com

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.{Json, Writes}

object LoadJsonToHiveTables extends App
{

  val conf = new SparkConf().setAppName("bbs.becpsn.com.LoadJsonToHiveTables")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  val sqlContext = SQLContext.getOrCreate(sc)
  val options :Map[String, String] = parseOption(Map[String, String](), args.toList)
  //validateOpts(opts)
  printOptions

  def printOptions = {
    println("Spark job submitted with configurations")
    options.keySet.foreach( k => {
      println(k + "=>" + options.getOrElse(k,""))
    })
  }
  val jobName: String = options.getOrElse[String]("jobname", "bbs.becpsn.com.LoadJsonToHiveTables")
  val source: String = options.getOrElse[String]("hdfs-source","not specified")
  var minPartition: String = options.getOrElse[String]("number-of-partitions", "2")

  val broker:String = options.getOrElse[String]("kafka-broker", "192.166.4.171:6667")
  val zkUrl: String = options.getOrElse[String]("zookeeper", "192.166.4.171:2181")
  def kafkaProducerProperties(kafkaBroker: String,zookeeperUrl: String,id: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaBroker)
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

  def processFile(opts: Map[String, String], file: String) = {
    var dbName:String = opts.getOrElse[String]("hive-schema","bbs_sap_pr")

    val dbNameOps :String= opts.getOrElse[String]("hive-operations-schema", "bbs_sap_ops")
    val hive_schema:String = opts.getOrElse[String]("hive-schema", "bbs_sap_pr")
    val ingestionSummaryTopic:String = opts.getOrElse[String]("ingestion-summary-topic", "ingestion-summary")
    val deadMessagesTopic:String = opts.getOrElse[String]("dead-messages-topic", "dead-messages")
    val insertTime = System.currentTimeMillis()
    val filename = FilenameUtils.getBaseName(file)
    val fnregexpattern = """(\w+)-([A-Za-z]+)([0-9]+)""".r
    val fnregexpattern(table,opr_ind,genTime) = filename

    try {
      val generationTime = genTime match{
        case "" => System.currentTimeMillis()
        case _ => dateFormat.parse(genTime).getTime
      }
      val df = sqlContext.read.json(file)
      //      import sqlContext.implicits._
      //      val currentTime = (() => System.currentTimeMillis().toString)
      import org.apache.spark.sql.functions

      val tableWithSchema=dbName + "." + table
      if(!df.take(1).isEmpty) {

        if (df.columns.contains("_corrupt_record")) {

          val df1 = df.filter("`_corrupt_record` is null").drop(df.col("_corrupt_record"))
          if(!df1.take(1).isEmpty) {
            val df2 = df1.withColumn("inserttime", functions.lit(genTime))
            df2.printSchema()
            df2.write.mode("append").format("orc").saveAsTable(tableWithSchema)
            val timeToInsert = filename.split("-").take(1).mkString.replace(".json","").replace("UPD","").replace("INS","").replace("DEL","")

            val tableIngestSummary = TableIngestSummary(dbName, table, file,opr_ind, "batch", df2.count(), "", genTime, insertTime)
            val msg = play.api.libs.json.Json.toJson(tableIngestSummary).toString()
            val dataRecord = new ProducerRecord[String, String](ingestionSummaryTopic, null, msg)
            tableInsertSummaryProducer.send(dataRecord)
          }
          val df3 = df.selectExpr("`_corrupt_record`").filter("`_corrupt_record` is not null").withColumn("table", functions.lit(tableWithSchema)).withColumn("inserttime", functions.lit(genTime))
          df3.write.mode("append").format("orc").saveAsTable(dbNameOps + ".corrupt_records")

        } else {
          val df1 = df.withColumn("inserttime", functions.lit(genTime))
          df1.printSchema()
          df1.write.mode("append").format("orc").saveAsTable(tableWithSchema)
          val tableIngestSummary = TableIngestSummary(dbName, table, filename, opr_ind, "batch", df1.count(), "", genTime, insertTime)

          val msg = play.api.libs.json.Json.toJson(tableIngestSummary).toString()
          val dataRecord = new ProducerRecord[String, String](ingestionSummaryTopic, null, msg)
          tableInsertSummaryProducer.send(dataRecord)
        }
      }
    } catch {
      case ex => {
        val tableIngestSummary = TableIngestSummary(dbName, "", "", "batch","",0, "Failed processesing " + ex.toString(), genTime, insertTime)
        val msg = play.api.libs.json.Json.toJson(tableIngestSummary).toString()
        val errored = new ProducerRecord[String, String](deadMessagesTopic, tableIngestSummary.tablename, msg)
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
    processFile(options, a)


  })
  tableInsertSummaryProducer.close()
  deadLetterProducer.close()

  def parseOption(opts: Map[String, String], args: List[String]): Map[String, String] = args match {
    case "-h" :: optarg :: tail => parseOption(opts ++ Map("hdfs-source" -> optarg.toString), tail)
    case "-j" :: optarg :: tail => parseOption(opts ++ Map("jobname" -> optarg.mkString), tail)
    case "-z" :: optarg :: tail => parseOption(opts ++ Map("zookeeper" -> optarg.mkString), tail)
    case "-kb" :: optarg :: tail => parseOption(opts ++ Map("kafka-broker" -> optarg.mkString), tail)
    case "-hs" :: optarg :: tail => parseOption(opts ++ Map("hive-schema" -> optarg.mkString), tail)
    case "-ops" :: optarg :: tail => parseOption(opts ++ Map("hive-operations-schema" -> optarg.mkString), tail)
    case "-ist" :: optarg :: tail => parseOption(opts ++ Map("ingestion-summary-topic" -> optarg.mkString), tail)
    case "-dlt" :: optarg :: tail => parseOption(opts ++ Map("dead-messages-topic" -> optarg.mkString), tail)
    case "-n" :: optarg :: tail => parseOption(opts ++ Map("number-of-partitions" -> optarg.mkString), tail)
    case Nil => opts
    case s :: tail => usage(-1, s"Option $s not recognised")
  }

  def validateOpts(opts: Map[String, String]) = {
    if (!opts.contains("hdfs-source")) {
      println("No server hostname supplied [-h]")
      usage(-1)
    }

  }

  def usage(code: Int, msg: String = "") = {
    val usg =
      s"Usage: ${LoadJsonToHiveTables.getClass.getName} -h HDFS-SOURCE -j JOBNAME " +
        s"-z ZOOKEEPER -kb KAFKA-BROKER -hs HIVE-SCHEMA -ops HIVE-OPERATION-SCHEMA -ist INJESTION-SUMMARY-TOPIC -dlt DEAD-LETTER-TOPIC -n NUMBER-OF-PARTITIONS"
    if (!msg.isEmpty) println(msg)
    println(usg)
    sys.exit(code)
  }



}

