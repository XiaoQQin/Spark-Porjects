import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constant
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
object PageConvertStat {


  def main(args: Array[String]): Unit = {
    // 获取json参数
    val jsonStr = ConfigurationManager.config.getString(Constant.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    //获取唯一主键
    val taskUUID = UUID.randomUUID().toString
    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")
    //创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取用户行为数据
    val sessionId2ActionRDD = getUserVisitAction(sparkSession, taskParam)
    //获取给定的page页面
    val pageFlowStr = ParamUtils.getParam(taskParam, Constant.PARAM_TARGET_PAGE_FLOW)
    // pageFlowArray :Array[1,2,3,4,5,6,7]
    val pageFlowArray = pageFlowStr.split(",")
    //pageFlowArray.slice(0, pageFlowArray.length - 1):Array[1,2,3,4,5,6]
    //pageFlowArray.tail:Array[1,2,3,4,5,6,7]
    //targetPageSplit:[1_2,2_3,3_4...]
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }
    //进行groupByKey
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    // 数据转化,获取相对应的RDD
    //pageSplitNumRDD：RDD[(String,Long)]
    val pageSplitNumRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) => {
        //将一个session内的所有action按照actionTime进行排序
        val sortList = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime > DateUtils.parseTime(item2.action_time).getTime
        })
        //获取所有action访问的page
        val pageList = sortList.map {
          case action => action.page_id
        }
        // 进行拼接成 1_2 样式
        val pageSplit = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }
        // 根据要统计的目标进行过滤
        val pageSplitFilter = pageSplit.filter {
          case item => targetPageSplit.contains(item)
        }
        // 进行map
        pageSplitFilter.map {
          case item => (item, 1L)
        }
      }
    }
    //进行统计
    val pageSplitCountMap = pageSplitNumRDD.countByKey()
    // 开始页面  startPage=0
    val startPage = pageFlowArray(0).toLong
    // 获取开始页面为0的action
    val startPageCount = sessionId2ActionRDD.filter {
      case (sessionId, action) => action.page_id == startPage
    }.count()
    getPageConvert(sparkSession,taskUUID,targetPageSplit,startPageCount,pageSplitCountMap)

  }
  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String, targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap: collection.Map[String, Long])={
    //初始化一个map，用来存储结果
    val pageSplitRatio = new mutable.HashMap[String, Double]()

    var lastPageCount = startPageCount.toDouble
    for(pageSplit <- targetPageSplit){
      // 获取目标 page1_page2 页面的出现次数
      val currentPageSplitCount = pageSplitCountMap.get(pageSplit).get.toDouble
      //获取比例
      val ratio = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit,ratio)
      lastPageCount=currentPageSplitCount
    }

    // 即使用 | 进行分隔，拼接成一个长字符串
    val convertStr = pageSplitRatio.map {
      case (pageSplit, ratio) => pageSplit + "=" + ratio
    }.mkString("|")
    //封装成一个对象
    val pageSplit = PageSplitConvertRate(taskUUID, convertStr)
    //转化为RDD
    val pageSplitRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))
    //写入数据库中
    import  sparkSession.implicits._
    pageSplitRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constant.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constant.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constant.JDBC_PASSWORD))
      .option("dbtable","page_convert_ratio")
      .mode(SaveMode.Append)
      .save()
  }

  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject)={
    //获取开始与结束时间
    val startDate = ParamUtils.getParam(taskParam, Constant.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constant.PARAM_END_DATE)

    val sql="select * from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"'"
    import  sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id,item))
  }
}
