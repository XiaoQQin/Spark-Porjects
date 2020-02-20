import java.util.UUID

import commons.model.{ProductInfo, UserInfo, UserVisitAction}
import commons.utils.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object VisitDataGenerator {

  // define some tables
  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"

  /**
   * generate user visit data
   * @return
   */
  private def mockUserVisitActionData()={

    val searchKeywords = Array("apple iphone", "computer", "ipad", "headset", "Lamer", "deep learning", "apple", "spark", "food")
    val date = DateUtils.getTodayDate()
    val actions = Array("search", "click", "order", "pay")
    val random = new Random()
    val rows = ArrayBuffer[UserVisitAction]()
    //100 users
    for(i <- 0 to 100){
      val userID = random.nextInt(100)
      //every user generate 10 rows
      for(j<- 0 to 10){
        //generate unqiue sessionID
        val sessionID = UUID.randomUUID().toString.replace("_", "")
        val baseActionTime = date + " " + random.nextInt(23)
        // every (userID+sessionID) generate 0-100 visit rows
        for(k <- 0 to random.nextInt(100)){
          val pageID = random.nextInt(10)
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid = random.nextInt(10).toLong
          // get the random action
          val action = actions(random.nextInt(4))

          action match{
            case "search" => searchKeyword=searchKeywords(random.nextInt(searchKeywords.length))
            case "click"  => clickCategoryId=random.nextInt(100).toLong
              clickProductId=random.nextInt(100).toLong
            case "order"  => orderCategoryIds=random.nextInt(100).toString
              orderProductIds=random.nextInt(100).toString
            case "pay"    => payCategoryIds=random.nextInt(100).toString
              payProductIds=random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userID, sessionID,
            pageID, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityid)

        }
      }
    }
    rows.toArray

  }

  /**
   * generate user info data
   * @return
   */
  private def mockUserInfo(): Array[UserInfo] = {

    val rows = ArrayBuffer[UserInfo]()
    val sexes = Array("male", "female")
    val random = new Random()

    // random create 100 user info data
    for (i <- 0 to 100) {
      val userid = i
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))
      rows += UserInfo(userid, username, name, age,
        professional, city, sex)
    }
    rows.toArray
  }

  private def mockProductInfo()={
    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()
    val productStatus = Array(0, 1)

    for (i <- 0 to 100){

      val productid=i
      val productName = "product" + i
      val extendInfo="{\"product_status\": " + productStatus(random.nextInt(2)) + "}"
      rows +=ProductInfo(productid,productName,extendInfo)
    }
    rows.toArray
  }

  /**
   * insert DataFrame data to Hive
   * @param spark
   * @param tableName
   * @param dataDF
   */
  private def insertHive(spark:SparkSession,tableName:String,dataDF:DataFrame)={
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  def main(args: Array[String]): Unit = {
    // create spark conf
    // 创建spark配置
    val sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")
    // create sparkSession
    // 创建 sparkSession
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //Generate Data
    //产生数据
    val userVisitActionsData = this.mockUserVisitActionData()//用户行为数据
    val userInfoData = this.mockUserInfo() //用户信息数据
    val productInfoData = this.mockProductInfo()//产品信息数据

    //transform to RDD
    //转化为RDD
    val userVisitActionRDD = spark.sparkContext.makeRDD(userVisitActionsData)
    val userInfoRDD = spark.sparkContext.makeRDD(userInfoData)
    val productInfoRDD = spark.sparkContext.makeRDD(productInfoData)

    import spark.implicits._

    // transform to DataFrame and save in Hive
    // 存储进Hive
    val userVisitActionDF = userVisitActionRDD.toDF()
    insertHive(spark,USER_VISIT_ACTION_TABLE,userVisitActionDF)
    val userInfoDF = userInfoRDD.toDF()
    insertHive(spark,USER_INFO_TABLE,userInfoDF)
    val productInfoDF = productInfoRDD.toDF()
    insertHive(spark,PRODUCT_INFO_TABLE,productInfoDF)
    spark.close()
  }


}
