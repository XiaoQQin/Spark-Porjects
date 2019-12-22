import dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import utils.DateUtils

import scala.collection.mutable.ListBuffer

object StatusStreamingApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(60))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop132:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("streamingtopic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

//    stream.map(_.value()).count().print()

    //    wordCount.print()

    val logs: DStream[String] = stream.map(_.value())

    val cleanData: DStream[ClickLog] = logs.map(line => {
      //split the sting
      // line:
      //  167.187.55.46   19-12-19 13:31:22       "GET /class/130.html HTTP/1.1"  https://search.yahoo.com/search?p=Hadoop Framework      404
      //  143.156.10.46   19-12-19 13:31:22       "GET /class/146.html HTTP/1.1"  -                                                       500
      val infos: Array[String] = line.split("\t")

      // infos(2)="GET /class/131.html HTTP/1.1"
      // url= /class/131.html
      val url: String = infos(2).split(" ")(1)
      var courseId = 0

      if (url.startsWith("/class")) {

        val courseIdHTML: String = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(4).toInt, infos(3))

    }).filter(clicklog => clicklog.courseId != 0)


//    cleanData.print()
    //put the data in hbase

    cleanData.map(x=>{
      (x.time.substring(0,8)+"_"+x.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list = new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair=>{
          list.append(CourseClickCount(pair._1,pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })


    cleanData.map(x=>{
      val referer: String = x.referer.replaceAll("//","/")

      val splits: Array[String] = referer.split("/")
      var host=""
      if (splits.length>2){
        host=splits(1)
      }
      (host,x.courseId,x.time)
    }).filter(_._1!="").map(x=>{
      (x._3.substring(0,8)+"_"+x._1+"_"+x._2,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list_search = new ListBuffer[CourseSearchClickCount]
        partitionRecords.foreach(pair=>{
          list_search.append(CourseSearchClickCount(pair._1,pair._2))
        })

        CourseSearchClickCountDAO.save(list_search)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}


