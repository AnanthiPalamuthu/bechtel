package com.hashmap.bechtel.app.sap.streaming

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SQLContext}// SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.hive._

trait SparkApplicationContext {

//     val conf = new SparkConf().setAppName("StreamingSparkApp").setMaster("local[1]")
     val conf = new SparkConf().setAppName("StreamingSparkApp")
     val sc = new SparkContext(conf)
     val ssc = new StreamingContext(sc, Seconds(2))
     val hiveContext = new HiveContext(sc)
     val sqlContext = SQLContext.getOrCreate(sc)
     
}
