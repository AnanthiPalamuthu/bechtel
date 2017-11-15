package com.hashmap.bktl.currentview

import org.apache.spark.sql.SparkSession

object CurrentView {
  def main(args: Array[String]) {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "hdfs://hdp1.hashmap.net:8020/apps/hive/warehouse"

    val spark = SparkSession.builder
      //.master("local[2]")
      .appName("Spark-Catalog-Example")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.warehouse.dir", warehouseLocation)
      .getOrCreate()

    //interacting with catalogue

    val catalog = spark.catalog

    //print the databases

    //catalog.listDatabases().foreach { x => println(x) }
    catalog.setCurrentDatabase("default")
    //catalog.listTables.show
    //catalog.listColumns("pa0021").foreach { x => println(x) }

    import spark.implicits._
    import spark.sql

    // sql("SELECT * FROM pa0021").limit(10).show()

    val tablename = "pa0021"
    //  ins and upd
    val updinsDf = sql(s"""SELECT pernr, begda, endda, subty, concat(pernr,begda, endda,subty) ui_pbes FROM $tablename WHERE opr_ind in ('INS','UPD')""")
    updinsDf.createOrReplaceTempView("updins_view")

    val delDf = sql(s"""SELECT pernr, begda, endda, subty, concat(pernr,begda, endda,subty) d_pbes FROM $tablename WHERE opr_ind in ('DEL')""")
    delDf.createOrReplaceTempView("del_view")

    val remainingDF = sql (""" select * from updins_view left anti join del_view on ui_pbes = d_pbes""")
    remainingDF.createOrReplaceTempView("remaining_view")
    remainingDF.show()
    val sel_view = sql(""" select * from remaining_view where pernr = "00141560" """)

    sel_view.show

    val residuesDf = sql(
      s""" select distinct t1.pernr, t1.begda, t1.endda, t1.subty, t1.inserttime from $tablename t1 join remaining_view v1
         |on t1.pernr=v1.pernr and t1.begda=v1.begda and t1.endda=v1.endda and t1.subty=v1.subty
         | order by t1.inserttime desc""".stripMargin)

    residuesDf.show()
    residuesDf.createOrReplaceTempView("residues_view")

    val rankedUpdInsDf = sql(s""" select pernr, begda, endda, subty, inserttime, opr_ind,
                                 	rank() over ( partition by pernr, begda, endda, subty order by inserttime desc, opr_ind desc) as rank
                                 	from $tablename where (unix_timestamp (ENDDA, 'yyyyMMdd') > FROM_UNIXTIME(UNIX_TIMESTAMP()) or endda='99991231') and opr_ind != 'DEL' """)

    rankedUpdInsDf.createOrReplaceTempView("rankedUpdIns_view")
    rankedUpdInsDf.show()

    val toprankendDf = sql(
      s""" select distinct t1.* from $tablename t1
         |left join rankedUpdIns_view v1 on t1.pernr=v1.pernr and t1.begda=v1.begda and t1.endda=v1.endda and t1.subty=v1.subty and t1.inserttime=v1.inserttime
         |where v1.rank=1 """.stripMargin)

    toprankendDf.createOrReplaceTempView("topranked_view")
    toprankendDf.show()
    toprankendDf.explain()
    toprankendDf.write.format("ORC").saveAsTable("bbs_sap.pa0021_final")
  }
}
