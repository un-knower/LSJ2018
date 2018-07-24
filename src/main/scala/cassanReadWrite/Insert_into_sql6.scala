package cassanReadWrite

import libsvm.PackageChecker
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Insert_into_sql6 {
  def main(args: Array[String]): Unit = {

    val packages = List("org.apache.spark:spark-core_2.21:2.2.0",
      "org.apache.spark:spark-sql_2.11:2.2.0",
      "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3")
    val jars = new PackageChecker(packages.mkString(","), "", "").getJar()
    val alljars = jars.split(",") ++ Seq("C:\\Users\\lisj1\\IdeaProjects\\Uimge02\\out\\artifacts\\Uimge02_jar\\Uimge02.jar")

    val conf = new SparkConf()
      .setMaster("spark://10.90.9.111:7077")
      .setAppName("Uimge_DLevel_insert_into_manualapp2")
      .set("spark.cassandra.connection.host", "10.80.17.155")
      .set("spark.executor.memory", "30g")
      .set("spark.cores.max", "280")
      .setJars(alljars)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val vectorT1 = List("IT", "奥运", "棒球", "冰雪项目", "财经", "创业", "大陆人事", "大陆时事", "电视娱乐", "电影", "动漫", "动物世界", "法制", "反腐", "房产", "风水", "佛教", "橄榄球", "港澳", "高尔夫", "高考", "高科技产业", "搞笑", "公务员", "公益", "国际", "互联网", "家居", "家庭教育", "健康", "健身", "军事", "考古", "考研", "科技", "科学探索", "篮球", "历史", "两性", "留学", "旅游", "美女", "美食", "萌宠", "民生", "明星", "排球", "乒乓球", "奇闻轶事", "汽车", "亲子", "情感", "拳击", "赛车", "商学院教育", "社会", "社会八卦", "摄影", "生活", "时尚", "时政", "收藏", "数码", "台球", "台湾", "跆拳道", "太极拳", "体操", "体育", "天气", "田径", "通信业", "网球", "围棋", "文化", "星座", "学前教育", "演出", "移民", "音乐", "游戏", "游泳跳水", "娱乐", "宇宙大观", "羽毛球", "在线教育", "早教", "战争历史", "职场", "职业培训", "中小学教育", "自行车", "足球")

    import spark.implicits._
    val df1 = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimg_keyvalues2", "keyspace" -> "dmp"))
      .load()
      .select($"uid",$"t1")
/*
    val df2 = df1.map(row => {
      val uid = row(0)
      val t1List = row(1).toString
      var wordList = List[String]()
      row(1)
    })*/
/*
    val vectorT1 = List("IT", "奥运", "棒球", "冰雪项目", "财经", "创业", "大陆人事", "大陆时事", "电视娱乐", "电影", "动漫", "动物世界", "法制", "反腐", "房产", "风水", "佛教", "橄榄球", "港澳", "高尔夫", "高考", "高科技产业", "搞笑", "公务员", "公益", "国际", "互联网", "家居", "家庭教育", "健康", "健身", "军事", "考古", "考研", "科技", "科学探索", "篮球", "历史", "两性", "留学", "旅游", "美女", "美食", "萌宠", "民生", "明星", "排球", "乒乓球", "奇闻轶事", "汽车", "亲子", "情感", "拳击", "赛车", "商学院教育", "社会", "社会八卦", "摄影", "生活", "时尚", "时政", "收藏", "数码", "台球", "台湾", "跆拳道", "太极拳", "体操", "体育", "天气", "田径", "通信业", "网球", "围棋", "文化", "星座", "学前教育", "演出", "移民", "音乐", "游戏", "游泳跳水", "娱乐", "宇宙大观", "羽毛球", "在线教育", "早教", "战争历史", "职场", "职业培训", "中小学教育", "自行车", "足球")

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
      }
      (uid,wordList.distinct.sortWith((a, b) => a.split(":")(0).toInt < b.split(":")(0).toInt))
      //      tag.saveToCassandra("uimg", "test7")
    })
    */

    df1.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "uimge_manual_app2", "keyspace" -> "uimg"))
      .mode(SaveMode.Append)
      .save()

  }


}
