package cassanReadWrite

import libsvm.PackageChecker
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object Insert_into_sql4 {
  def main(args: Array[String]): Unit = {

    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("Uimge_DLevel_insert_into_manualapp2")
      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.executor.memory", "30g")
      .set("spark.cores.max", "280")
      .setJars(alljars)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
        val df1 = spark
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "uimge_manual_app2", "keyspace" -> "uimg"))
          .load()
          .select($"uid"
            ,expr("1").as[List[String]].alias("d107")
            ,expr("1").alias("d203")
            ,expr("1").alias("d306")
            ,expr("1").alias("d407")
            ,expr("1").alias("d505")
            ,expr("1").alias("d601")
            ,expr("1").alias("d602")
            ,expr("1").alias("d603")
            ,expr("1").alias("d604")
            ,expr("1").alias("d605")
            ,expr("1").alias("d606")
            ,expr("1").alias("d607")
            ,expr("1").alias("d608")
            ,expr("1").alias("d609")
            ,expr("1").alias("d610")
            ,expr("1").alias("d701")
            ,expr("1").alias("d702")
            ,expr("1").alias("d703")
            ,expr("1").alias("d704")
            ,expr("1").alias("d705")
            ,expr("1").alias("d706")
            ,expr("1").alias("d707")
            ,expr("1").alias("d708")
            ,expr("1").alias("d801")
            ,expr("1").alias("d802")
            ,expr("1").alias("d803")
            ,expr("1").alias("d804")
            ,expr("1").alias("d805")
            ,expr("1").alias("d806")
            ,expr("1").alias("d807")
            ,expr("1").alias("d901")
            ,expr("1").alias("d902")
            ,expr("1").alias("d903")
            ,expr("1").alias("d904")
            ,expr("1").alias("d905")
            ,expr("1").alias("d906")
            ,expr("1").alias("d907")
            ,expr("1").alias("d908")
            ,expr("1").alias("d1001")
            ,expr("1").alias("d1002")
            ,expr("1").alias("d1003")
            ,expr("1").alias("d1004")
            ,expr("1").alias("d1005")
            ,expr("1").alias("d1006")
            ,expr("1").alias("d1007")
            ,expr("1").alias("d1008")
            ,expr("1").alias("d1101")
            ,expr("1").alias("d1102")
            ,expr("1").alias("d1103")
            ,expr("1").alias("d1104")
            ,expr("1").alias("d1105")
            ,expr("1").alias("d1106")
            ,expr("1").alias("d1107")
            ,expr("1").alias("d1108")
            ,expr("1").alias("d1109")
            ,expr("1").alias("d1201")
            ,expr("1").alias("d1202")
            ,expr("1").alias("d1203")
            ,expr("1").alias("d1204")
            ,expr("1").alias("d1205")
            ,expr("1").alias("d1206")
            ,expr("1").alias("d1207")
            ,expr("1").alias("d1301")
            ,expr("1").alias("d1302")
            ,expr("1").alias("d1303")
            ,expr("1").alias("d1304")
            ,expr("1").alias("d1305")
            ,expr("1").alias("d1306")
            ,expr("1").alias("d1307")
            ,expr("1").alias("d1308")
            ,expr("1").alias("d1309")
            ,expr("1").alias("d1310")
            ,expr("1").alias("d1401")
            ,expr("1").alias("d1402")
            ,expr("1").alias("d1403")
            ,expr("1").alias("d1405")
            ,expr("1").alias("d1406")
            ,expr("1").alias("d1407")
            ,expr("1").alias("d1408")
            ,expr("1").alias("d1409")
            ,expr("1").alias("d1501")
            ,expr("1").alias("d1502")
            ,expr("1").alias("d1503")
            ,expr("1").alias("d1504")
            ,expr("1").alias("d1505")
            ,expr("1").alias("d1601")
            ,expr("1").alias("d1602")
            ,expr("1").alias("d1603")
            ,expr("1").alias("d1604")
            ,expr("1").alias("d1605")
            ,expr("1").alias("d1606")
            ,expr("1").alias("d1607")
            ,expr("1").alias("d1608")
            ,expr("1").alias("d1701")
            ,expr("1").alias("d1702")
            ,expr("1").alias("d1703")
            ,expr("1").alias("d1704")
            ,expr("1").alias("d1801")
            ,expr("1").alias("d1802")
            ,expr("1").alias("d1803")
            ,expr("1").alias("d1804")
            ,expr("1").alias("d1805")
          )

    df1.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_manual_app2", "keyspace" -> "uimg"))
      .mode(SaveMode.Append)
      .save()

    /*    import spark.implicits._
        val df1 = spark.read.format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "uimge_manual_app2", "keyspace" -> "uimg"))
          .load()
          .select($"uid", expr("0").alias("c1100"))

        df1.write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "uimge_manual_app2", "keyspace" -> "uimg"))
          .mode(SaveMode.Append)
          .save()*/

    /*    import spark.implicits._
        val df2 = spark.read.format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "new_game_group", "keyspace" -> "groups"))
          .load().filter($"label" > 0.5)
          .select($"uid", expr("1").alias("c1100"))

        df2.write
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "uimge_manual_app2", "keyspace" -> "uimg"))
          .mode(SaveMode.Append)
          .save()*/

/*    import spark.implicits._
    val df3 = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimg_keyvalues2", "keyspace" -> "dmp"))
      .load()

    df3.createOrReplaceTempView("table")
    val query = spark.sql(    "select uid,firstin as first_in,ip,iphone," +
      "lastin as last_in,lastt1 as last_t1,lastt2 as last_t2,lastt3 as last_t3," +
      "lastutime as last_utime, recentt1 as recent_t1,recentt2 as recent_t2," +
      "recentt3 as recent_t3,slidet1 as slide_t1,slidet2 as slide_t2,slidet3 as slide_t3," +
      "t1,t2,t3,ua,uar as ua_r,uav as ua_v,utime,vidt1 as vid_t1,vidt2 as vid_t2,vidt3 as vid_t3 from table")

    val query1 = spark.sql(    "select uid,ua,utime,t1,t2,t3 from table")

    query1.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_values", "keyspace" -> "interest"))
      .mode(SaveMode.Append)
      .save()*/

/*    val df4 = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimg_keyvalues2", "keyspace" -> "dmp"))
      .load()
      .select("uid","ua","utime","t1","t2","t3")

    df4.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_values", "keyspace" -> "interest"))
      .mode(SaveMode.Append)
      .save()*/
  }
}
