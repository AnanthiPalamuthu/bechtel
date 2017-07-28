package com.hashmap.bechtel.app.sap.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils


object StreamData extends App
  with SparkApplicationContext
  with KafkaConfigurations {

  private lazy val log = Logger.getLogger(getClass)

   if (args.length != 3) {
         log.error("Job accepts 3 parameters <ProcesseDataTopic> <Broker> <zkkafka>")
       }

    
  val tagDataRawTopic = Set(args(0))
  val broker = args(1)
  val zkUrl = args(2)
  
  log.info("****************create stream****************")

   val tagDataStream = KafkaUtils.createDirectStream[String,String, StringDecoder, StringDecoder](ssc, kafkaConsumerProperties(broker,zkUrl), tagDataRawTopic)
   val tagDataRecords = tagDataStream.map(_._2)  
       
  tagDataRecords.foreachRDD {

       record=>{
       val dfs = sqlContext.read.json(record)
            System.out.println("****************dfs_schema****************")
             dfs.printSchema()
         dfs.write.mode("append")saveAsTable("p041")

    }
  }
  ssc.start()
  ssc.awaitTermination()
  log.info("****************done****************")
}
