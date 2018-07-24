import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession, hive}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.expr

object MultiAppItemCassan3 extends App {
  val conf = new SparkConf()
    .setMaster("spark://10.90.9.111:7077")
    .set("spark.executor.memory", "20g")
    .set("spark.cores.max", "200")

  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .config("spark.hadoop.hive.metastore.uris", "thrift://10.90.9.112:9083")
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext
  sc.hadoopConfiguration.addResource("conf/hadoop/core-site.xml")
  sc.hadoopConfiguration.addResource("conf/hadoop/hdfs-site.xml")

  import spark.implicits._
  import spark.sql

  val query1 = spark.sql("select key," +
    "get_json_object(value,'$.title') as title, " +
    "get_json_object(value,'$.category') as category " +
    "from pagekv where get_json_object(value,'$.title') is not null")
    .select($"key",$"category".as[List[String]].alias("category"))
//  val query2 = query1.map{ case Row(key:String, value:String) => s"Key: $key, Value: json_parse($value)"}
  query1.show(10)

/*  val appitemdf =  spark
    .read
    .options(Map( "table" -> "pagekv", "keyspace" -> "default"))
    .load()
  .select($"key".as[List[String]]
    ,$"value".as[List[String]].alias("d107"))
*/
  spark.setCassandraConf("Cluster1",
    CassandraConnectorConf.ConnectionHostParam
      .option("10.80.17.155,10.80.18.155,10.80.19.155,10.80.20.155,10.80.21.155,10.80.22.155,10.80.23.155,10.80.24.155,10.80.25.155"))

  query1.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "articletest1", "keyspace" -> "groups" , "cluster" -> "Cluster1"))
    .mode(SaveMode.Append)
    .save()


 //////////////////////////////////////////////////////////////////////
/*  select split
    (regexp_replace
      (regexp_extract
         ('[{"bssid":"6C:59:40:21:05:C4","ssid":"MERCURY_05C4"},{"bssid":"AC:9C:E4:04:EE:52","appid":"10003","ssid":"and-Business"}]',
          '^\\[(.+)\\]$',
          1
         ),'\\}\\,\\{', '\\}\\|\\|\\{'
      ),'\\|\\|'
    ) as str
  from dual

  regexp_extract(get_json_object(value,'$.category'),'^\\[(.+)\\]$')*/


  spark.stop

}
