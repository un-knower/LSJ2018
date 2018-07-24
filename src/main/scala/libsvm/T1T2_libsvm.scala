package libsvm

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.SaveMode
//import libsvm.PackageChecker
import org.apache.spark.{SparkConf, SparkContext}

object T1T2_libsvm extends App {
//  def main(args: Array[String]): Unit = {

    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",")
    //    ++ Seq("C:\\Users\\lisj1\\IdeaProjects\\Uimge02\\out\\artifacts\\Uimge02_jar\\Uimge02.jar")

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("UimgeManualApp")
      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.executor.memory", "10g")
      //      .set("spark.executor.num","180")
      .setJars(alljars)

    val sc = new SparkContext(conf)
    val connector = CassandraConnector(conf)
    val rdd2 = sc.cassandraTable("dmp", "uimg_keyvalues2").select("uid","t1")

    val vectorT1 = List("IT", "奥运", "棒球", "冰雪项目", "财经", "创业", "大陆人事", "大陆时事", "电视娱乐", "电影", "动漫", "动物世界", "法制", "反腐", "房产", "风水", "佛教", "橄榄球", "港澳", "高尔夫", "高考", "高科技产业", "搞笑", "公务员", "公益", "国际", "互联网", "家居", "家庭教育", "健康", "健身", "军事", "考古", "考研", "科技", "科学探索", "篮球", "历史", "两性", "留学", "旅游", "美女", "美食", "萌宠", "民生", "明星", "排球", "乒乓球", "奇闻轶事", "汽车", "亲子", "情感", "拳击", "赛车", "商学院教育", "社会", "社会八卦", "摄影", "生活", "时尚", "时政", "收藏", "数码", "台球", "台湾", "跆拳道", "太极拳", "体操", "体育", "天气", "田径", "通信业", "网球", "围棋", "文化", "星座", "学前教育", "演出", "移民", "音乐", "游戏", "游泳跳水", "娱乐", "宇宙大观", "羽毛球", "在线教育", "早教", "战争历史", "职场", "职业培训", "中小学教育", "自行车", "足球")

    rdd2.map(row => {
      val uid = row.getString("uid")
      val t1List = row.getList[String]("t1")
      var wordList = List[String]()
      for (t1 <- t1List) {
        val str = t1.split("_")(0)
        val len = t1.split("_").length
        if (!(str == "null") && !(str == "$") && !(str == "") && !(str == " ")) {
          if (vectorT1.contains(str)) {
            val index = vectorT1.indexOf(str) + 1
            val t1String = index + ":" + t1.split("_")(len - 1)
            wordList :+= t1String
          }
        }
      }
      (uid,wordList.distinct.sortWith((a, b) => a.split(":")(0).toInt < b.split(":")(0).toInt))
//      tag.saveToCassandra("uimg", "test7")
    }).saveToCassandra("uimg","test", SomeColumns("uid","value"))
//  }
}

/*    val rdd3 = rdd2.map ( row => {
      val uid = row.get[String]("uid")
      var wordList = List[String]()
      for (t1 <- row.getList[String]("t1")) {
        val str = t1.split("_")(0)
        val len = t1.split("_").length
        if (!(str == "null") && !(str == "$") && !(str == "") && !(str == " ")) {
          if (vectorT1.contains(str)) {
            val index = vectorT1.indexOf(str) + 1
            val t1String = index + ":" + t1.split("_")(len - 1)
            wordList :+= t1String
          }
        }
      }*/
//      (uid,tag.mkString(","))
//      connector.withSessionDo(session => session.execute("INSERT INTO uimg.test2(uid, tag) VALUES (?, ?)", uid, tag.mkString(",")))
//      connector.withSessionDo(session => session.execute("INSERT INTO uimg.test2(uid) VALUES (?)", uid))

    /*
    for (row <- rdd2.collect) {
      val uid = row.getString("uid")
      val t1List = row.getList[String]("t1")
      var wordList = List[String]()
      for (t1 <- t1List) {
        val str = t1.split("_")(0)
        val len = t1.split("_").length
        if (!(str == "null") && !(str == "$") && !(str == "") && !(str == " ")) {
          if (vectorT1.contains(str)) {
            val index = vectorT1.indexOf(str) + 1
            val t1String = index + ":" + t1.split("_")(len - 1)
            wordList :+= t1String
          }
        }
      }

    rdd2.foreach(row => {
      val uid = row.getString("uid")
      val t1List = row.getList[String]("t1")
      var wordList = List[String]()
      for (t1 <- t1List) {
        val str = t1.split("_")(0)
        val len = t1.split("_").length
        if (!(str == "null") && !(str == "$") && !(str == "") && !(str == " ")) {
          if (vectorT1.contains(str)) {
            val index = vectorT1.indexOf(str) + 1
            val t1String = index + ":" + t1.split("_")(len - 1)
            wordList :+= t1String
          }
        }
      })

    for (row <- rdd2.collect) {
      val uid = row.getString("uid")
      val t1List = row.getList[String]("t1")
      var wordList = List[String]()
      for (t1 <- t1List) {
        val str = t1.split("_")(0)
        val len = t1.split("_").length
        if (!(str == "null") && !(str == "$") && !(str == "") && !(str == " ")) {
          if (vectorT1.contains(str)) {
            val index = vectorT1.indexOf(str) + 1
            val t1String = index + ":" + t1.split("_")(len - 1)
            wordList :+= t1String
          }
        }
      }

    // 去重+排序
    val tag = wordList.distinct.sortWith((a, b) => a.split(":")(0).toInt < b.split(":")(0).toInt)

    connector.withSessionDo(session => session.execute("INSERT INTO uimg.test2(uid, tag) VALUES (?, ?)", uid, tag.mkString(",")))
            */

