package cassanReadWrite

import libsvm.PackageChecker
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object InsertIntoSql7 {
  def main(args: Array[String]): Unit = {

    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("ArticleCategory_insert_into_test6")
      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.executor.memory", "20g")
      .set("spark.cores.max", "200")
      .setJars(alljars)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
        val df1 = spark
          .read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "test7", "keyspace" -> "uimg"))
          .load()
//          .filter("type='vivonex' or type='HuaweiP' or type='HuaweiMate'")
//          .select($"key",split($"category",",").as[List[String]].alias("category"))
//    .select($"key",split(regexp_replace($"category","(\\[)|(\\])|(\")",""),",").as[List[String]].alias("category"))
//  .select($"key".alias("pid"),
//          $"title",
//          split(regexp_replace($"category","[\\[,\\],\"]",""),",").as[List[String]].alias("category"))
      .select($"key".alias("pid"),
              $"title",
              split(regexp_replace($"category","(\\[)|(\\])|(\")",""),",").as[List[String]].alias("category"))

//    df1.createOrReplaceTempView("table")
//    val query1 = spark.sql("select key, regexp_replace(category,'\[|\]|\"') as category from table")

    df1.write
      .format("org.apache.spark.sql.cassandra")
//      .options(Map("table" -> "test6", "keyspace" -> "uimg"))
      .options(Map("table" -> "app_pages", "keyspace" -> "dmp"))
      .mode(SaveMode.Append)
      .save()

/*    import spark.sql
    val query1 = sql("select * from pagekv limit 10")
    val query2 = query1.map{ case Row(key:String, value:String) => s"Key: $key, Value: json_extract(json_parse($value),'category')"}
    query2.show(10)*/

  }


}
