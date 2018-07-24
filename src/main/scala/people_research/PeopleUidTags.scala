package people_research

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import libsvm.PackageChecker
import org.apache.spark.{SparkConf, SparkContext}

object PeopleUidTags {
  def main(args: Array[String]): Unit = {

    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("UimgeManualApp")
      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.executor.memory", "10g")
      .setJars(alljars)

    val sc = new SparkContext(conf)
    val connector = CassandraConnector(conf)
    val rdd = sc.cassandraTable("groups", "people_research")
//      .where("category_id <> ?,?,?", "z10001","z10003","z10004")

    for (row <- rdd.collect) {
      val uid = row.getString("uid")
      val category = row.getString("category")
      val category_id = row.getString("category_id")
      val c6 = row.getInt("c601")+row.getInt("c602")+row.getInt("c603")+row.getInt("c604")
      +row.getInt("c605")+row.getInt("c606")+row.getInt("c607")+row.getInt("c608")+row.getInt("c609")
      val c7 = row.getInt("c701")+row.getInt("c702")+row.getInt("c703")+row.getInt("c704")
      +row.getInt("c705")+row.getInt("c706")+row.getInt("c707")
      val c8 = row.getInt("c801")+row.getInt("c802")+row.getInt("c803")+row.getInt("c804")
      +row.getInt("c805")+row.getInt("c806")
      val c9 = row.getInt("c901")+row.getInt("c902")+row.getInt("c903")+row.getInt("c904")
      +row.getInt("c905")+row.getInt("c906")+row.getInt("c907")
      val c10 = row.getInt("c1001")+row.getInt("c1002")+row.getInt("c1003")+row.getInt("c1004")
      +row.getInt("c1005")+row.getInt("c1006")+row.getInt("c1007")
      val c11 = row.getInt("c1101")+row.getInt("c1102")+row.getInt("c1103")+row.getInt("c1104")
      +row.getInt("c1105")+row.getInt("c1106")+row.getInt("c1107")+row.getInt("c1108")
      val c12 = row.getInt("c1201")+row.getInt("c1202")+row.getInt("c1203")+row.getInt("c1204")
      +row.getInt("c1205")+row.getInt("c1206")
      val c13 = row.getInt("c1301")+row.getInt("c1302")+row.getInt("c1303")+row.getInt("c1304")
      +row.getInt("c1305")+row.getInt("c1306")+row.getInt("c1307")+row.getInt("c1308")+row.getInt("c1309")
      val c14 = row.getInt("c1401")+row.getInt("c1402")+row.getInt("c1403")
      +row.getInt("c1405")+row.getInt("c1406")+row.getInt("c1407")+row.getInt("c1408")
      val c15 = row.getInt("c1501")+row.getInt("c1502")+row.getInt("c1503")+row.getInt("c1504")
      val c16 = row.getInt("c1601")+row.getInt("c1602")+row.getInt("c1603")+row.getInt("c1604")
      +row.getInt("c1605")+row.getInt("c1606")+row.getInt("c1607")
      val c17 = row.getInt("c1701")+row.getInt("c1702")+row.getInt("c1703")
      val c18 = row.getInt("c1801")+row.getInt("c1802")+row.getInt("c1803")+row.getInt("c1804")

//      val tagList = List("教育_"+c6,"旅游_"+c7,"金融_"+c8,"汽车_"+c9,"房产_"+c10,
//        "游戏_"+c11,"体育_"+c12,"时尚网购_"+c13,"快消_"+c14,"美容时尚_"+c15,
//        "科技数码_"+c16,"母婴_"+c17,"健康美食_"+c18).mkString(",")


//      connector.withSessionDo(session => session.execute("INSERT INTO groups.people_taglist (category, uid, taglist) VALUES (?, ?, ?)", category, uid, tagList))
      connector.withSessionDo(session => session.execute("INSERT INTO groups.people_taglist (category, uid, taglist) VALUES (?, ?)", category, uid))
    }

  }
}
