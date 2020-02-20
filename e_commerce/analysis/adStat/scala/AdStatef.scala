import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constant
import commons.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ArrayBuffer
object AdStatef {




  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AdStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    //获取kafka的主机地址和主题
    val kafka_brokers = ConfigurationManager.config.getString(Constant.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constant.KAFKA_TOPICS)
    // 指定参数
    val kafkaParams = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      // auto.offset.reset
      // latest: 先从zookeeper 获取offset,如果有，直接使用，如果没有，从最新的数据开始消费
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // 创建Kafka连接 sparkStream
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array(kafka_topics), kafkaParams)
    )
    // 取出spark streaming 从kafka获取的每条数据的value值
    // adRealTimeValueDStream: DStream[RDD RDD RDD ]  adRealTimeValueDStream 里每一个数据为RDD，RDD每条数据为String
    // RDD[String]
    val adRealTimeValueDStream = stream.map(item => item.value())

    // transform 遍历的是每一个RDD
    val adRealTimeFilterDStream = adRealTimeValueDStream.transform {
      logRDD =>
        //获取所有的黑名单
        val blackListArray = AdBlacklistDAO.findAll()
        //获取所有黑名单的用户Id
        val userIdArray = blackListArray.map(item => item.userid)
        //对当前RDD进行过滤
        logRDD.filter {
          //每一条数据,不包括黑名单中的用户id即可
          case log =>
            val logSplit = log.split(" ")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }
    streamingContext.checkpoint("./spark-streaming")
    // checkpoint的间隔时间，注意必须是产生数据间隔时间的倍数
    adRealTimeFilterDStream.checkpoint(Duration(10000))
    //adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println))

    //需求一:实时维护黑名单
    generateBlackList(adRealTimeFilterDStream)

    //需求二:各省各城市一天中的广告点击量(累积统计)
    val key2ProvinceCityDStream = provinceCityClickStat(adRealTimeFilterDStream)
    //需求三：每天每隔省份的Top3热门广告
    proveinceTop3Advert(sparkSession,key2ProvinceCityDStream)

    //需求四: 最近一个小时广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    //adRealTimeFilterDStream:DStream[RDD[log]]
    //将获取的DStream 进行转化
    val key2NumStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        //获取每条数据的时间戳
        val timeStamp = logSplit(0).toLong
        //获取Key值 yyyymmdd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adid = logSplit(4).toLong
        //拼接字符串
        val key = dateKey + "_" + userId + "_" + adid
        (key, 1L)
    }
    //获取这个Dtream中每个人在某个广告一天内的点击数
    val key2CountDSteam = key2NumStream.reduceByKey(_ + _)
    //针对key2CountDSteam中的每个RDD
    key2CountDSteam.foreachRDD{
      rdd=>rdd.foreachPartition{
        items =>
          //items 为每个RDD中每个分区的多条数据
          val clickCountArray=new ArrayBuffer[AdUserClickCount]()
          for((key,value)<-items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val userId = keySplit(1).toLong
            val adid = keySplit(2).toLong
            //封装成对象加入到数组中
            clickCountArray+=AdUserClickCount(date,userId,adid,value)
          }
          //插入数据库中
          adUserClickCountDao.updateBatch(clickCountArray.toArray)
      }
    }

    //将点击数超过100的过滤掉
    val key2BlackListDStream = key2CountDSteam.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong
        //获取对应的点击数
        val clickCount = adUserClickCountDao.findClickCountByMultkey(date, userId, adid)
        if (clickCount > 100)
          true
        else
          false
    }
    //只获取userID，并且进行去重
    val userIdDStream = key2BlackListDStream.map {
      case (key, count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    // 将只有用户名的DStream加入到MySQL中
    userIdDStream.foreachRDD{
      rdd=>
        rdd.foreachPartition{
          items=>
            val userIdArray=new ArrayBuffer[AdBlacklist]()
            for(userId <- items)
              userIdArray += AdBlacklist(userId)
            AdBlacklistDAO.insertBatch(userIdArray.toArray)
        }

    }
  }

  /**
   * 各个城市的广告点击量统计
   * @param adRealTimeFilterDStream
   */
  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        //获取每条数据的时间戳
        val timeStamp = logSplit(0).toLong
        //获取Key值 yyyymmdd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val adid = logSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }
    //
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long] {
      //value 是当前key在DStream中所有的数据，state为在checkpoint中的key值对应维护的值
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
        if (state.isDefined)
          newValue = state.get
        for (value <- values)
          newValue += value
        Some(newValue)
    }

    key2StateDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items=>
          val adStateArray=new ArrayBuffer[AdStat]()
          for((key,count) <-items){
            val  keySplit=key.split("_")
            val date=keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adid = keySplit(3).toLong
            adStateArray+=AdStat(date,province,city,adid,count)

          }
          AdStatDAO.updateBatch(adStateArray.toArray)
      }
    }
    key2ProvinceCityDStream
  }





  def proveinceTop3Advert(sparkSession: SparkSession,
                          key2ProvinceCityDStream: DStream[(String, Long)]) = {
    //key2ProvinceCityDStream[RDD[(key,value)]]
    //转化key值
    val key2ProvinceCountDStream = key2ProvinceCityDStream.map {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)

        val newKey = date + "_" + province + "_" + adid
        (newKey, count)
    }
    //将相同key值进行相加
    val key2ProvinceAggrCountDStream = key2ProvinceCountDStream.reduceByKey(_ + _)

    val top3DateProvinceDStream = key2ProvinceAggrCountDStream.transform {
      rdd =>
        val basicDateRDD = rdd.map {
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong

            (date, province, adid, count)
        }
        import sparkSession.implicits._
        //创建临时表
        basicDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")
        val sql = "select date,province,adid,count from(" + "" +
          "select date,province,adid,count, row_number() over (partition by date,province order by count desc) rank from tmp_basic_info) t" +
          " where rank<=3"

        sparkSession.sql(sql).rdd
    }
    top3DateProvinceDStream.foreachRDD{
      rdd=>
        rdd.foreachPartition{
          items =>
            val top3Array=new ArrayBuffer[AdProvinceTop3]()
            for (item <- items){
              val date=item.getAs[String]("date")
              val province=item.getAs[String]("province")
              val adid=item.getAs[Long]("adid")
              val count=item.getAs[Long]("count")

              top3Array+=AdProvinceTop3(date = date,province = province,adid = adid,clickCount=count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
      }
    }
  }

  /**
   * 最近一个小时光广告点击量统计
   * @param adRealTimeFilterDStream
   */
  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    //转化key值
    val key2TimeMinuteDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adid = logSplit(4).toLong

        val key = timeMinute + "_" + adid
        (key, 1L)
    }

    val key2WindowDStreanm = key2TimeMinuteDStream.reduceByKeyAndWindow((a: Long, b: Long) => (a + b), Minutes(60), Minutes(1))

    key2WindowDStreanm.foreachRDD{
      rdd=>rdd.foreachPartition{
        items=>
          val trendArray=new ArrayBuffer[AdClickTrend]()
          for((key,count) <- items){
            val keySplit = key.split("_")
            val timeStamp=keySplit(0)
            val date=timeStamp.substring(0,8)
            val hour=timeStamp.substring(8,10)
            val minute=timeStamp.substring(10)

            val adid=keySplit(1).toLong
            trendArray+=AdClickTrend(date = date,hour=hour,minute=minute,adid=adid,clickCount = count)
          }
          AdClickTrendDAO.updateBatch(trendArray.toArray)
      }

    }
  }






}
