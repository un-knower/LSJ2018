/*
package com.ifeng.appitemsdb

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import shapeless._
import shapeless.syntax.sized._
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._

object Test10 extends App {
  val path = FP("lib")
  val conf = conf_parser(FP("conf/application.conf"), configstr =
    s"""
          current_path = ${path}
        """
  )
  println(path)
  val brokers = conf.getStringList("kafka.brokers")
  val brokers_str = brokers.mkString(",")
  val topic = conf.getString("kafka.topic")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> s"$brokers_str",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "uimg-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def jsonstomap(jsonstr: String): Map[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    val m = parse(jsonstr).extract[Map[String, String]]
    val m1 = m.map(s=>(s._1.toLowerCase,s._2))
    m1
  }

  //val sparkconf = new SparkConf().setAppName("Application").setMaster("local[2]")
  val sparkop = sparkopx(conf)
  val ssc = sparkop.get_ssc(600)
  val checkpointDirectory = "hdfs://advertidshadoop161v1taiji.cdn.ifengidc.com/dmp/sparkstreaming/"
  ssc.checkpoint(checkpointDirectory)
  //val ssc = new StreamingContext(sparkconf, Seconds(10))
  val topics = Array(topic)
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
  import uimgmapper._
  import scala.util.{Try, Success, Failure}
  val df = stream.map(record => {
    val jmap = Try(jsonstomap(record.value)) match {
      case  Success(m) => m
      case  Failure(_) => {
        println("failure:${record.value}")
        Map[String,String]()
      }
    }
    def mget(s:String):String =  {
      val v = jmap.get(s).getOrElse("")
      if(v == "") println(s)
      v
    }
    def mget1(s:String):List[String] =  {
      val value = jmap.get(s).getOrElse("")
      val all = value.split('#')
      all.toList
    }
    val ret = uimgmapper.fill_fields(mget, mget1)
    ret
  }).filter(_.uid != "")
  //df.print
  //val df = stream.map { consumerRecord => consumerRecord.value }.cache()
  df.saveToCassandra("dmp", "uimg_keyvalues2", get_uimgcols)
  ssc.start()
  ssc.awaitTermination()

*/
