import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constant
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object AreaTop3Stat {




  def main(args: Array[String]): Unit = {
    // 获取json参数
    val jsonStr = ConfigurationManager.config.getString(Constant.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    //获取唯一主键
    val taskUUID = UUID.randomUUID().toString
    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("area").setMaster("local[*]")
    //创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取城市和产品信息数据
    val cityId2ProductIdRDD = getCityAndPorductInfo(sparkSession, taskParam)
    //cityId2ProductIdRDD.foreach(println)
    //获取城市相关的RDD
    //cityIdAreaInfoRDD:RDD[(cityId,CityAreaInfo)]
    val cityIdAreaInfoRDD = getCityAreaInfo(sparkSession)
    cityIdAreaInfoRDD.foreach(println)
    getAreaPidBasicInfoTable(sparkSession,cityId2ProductIdRDD,cityIdAreaInfoRDD)
    sparkSession.sql("select * from temp_area_basic_info").show()

    //自定义一个uDF
    sparkSession.udf.register("concat_long_string",(v1:Long,v2:Long,split:String) =>{
      v1+split+v2
    })
    sparkSession.udf.register("group_concat_distinct",new GroupConcatDistinctUDAF)

    getAreaProductClickCountTable(sparkSession:SparkSession)
    sparkSession.sql("select * from tmp_arae_click_count").show()
  }

  def getAreaProductClickCountTable(sparkSession:SparkSession)={
    val sql="select area,pid,count(*) click_count," +
      "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_Infos"+
      " from temp_area_basic_info group by area,pid"
    sparkSession.sql(sql).createOrReplaceTempView("tmp_arae_click_count")
  }

  def getAreaPidBasicInfoTable(sparkSession: SparkSession,
                               cityId2ProductIdRDD: RDD[(Long, Long)],
                               cityIdAreaInfoRDD: RDD[(Long, CityAreaInfo)])={
    // 将两个RDD 进行join
    val areaPorductIdInfoRDD = cityId2ProductIdRDD.join(cityIdAreaInfoRDD).map {
      case (cityId, (productId, areaInfo)) => {
        (cityId, areaInfo.cityName, areaInfo.areaName, productId)
      }
    }

    import sparkSession.implicits._
    areaPorductIdInfoRDD.toDF("city_id","city_name","area","pid").createOrReplaceTempView("temp_area_basic_info")

  }
  def getCityAreaInfo(sparkSession: SparkSession)={
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map{
      case (cityId,cityName,area) =>{
        (cityId,CityAreaInfo(cityName,area))
      }
    }
  }
  def getCityAndPorductInfo(sparkSession: SparkSession, taskParam: JSONObject)={
    //获取开始与结束时间
    val startDate = ParamUtils.getParam(taskParam, Constant.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constant.PARAM_END_DATE)

    // 只获取发生过点击的action
    val sql="select city_id,click_product_id from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"' and click_product_id != -1"
    import  sparkSession.implicits._
    sparkSession.sql(sql).as[CityClickProduct].rdd.map{
      case cityPid => (cityPid.city_id,cityPid.click_product_id)
    }
  }


}
