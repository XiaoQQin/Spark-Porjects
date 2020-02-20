import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constant
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import javax.management.monitor.StringMonitor
import net.sf.json.JSONObject
import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SessionStatus {




  def main(args: Array[String]): Unit = {
    // 获取参数配置文件
    val jsonStr = ConfigurationManager.config.getString(Constant.TASK_PARAMS)
    // 转化为json
    val taskParam = JSONObject.fromObject(jsonStr)
    // 创造taskUUID
    val taskUUID = UUID.randomUUID().toString

    //初始化sparkSession
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //获取生成的原始数据并且转化为RDD
    val actionRDD = getOriActionRDD(sparkSession,taskParam)
    //进行map,转化为对应的RDD
    //sessionId2ActionRDD: RDD[(sessionId,UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))
    // 根据key值进行Group
    //sessionId2GroupActionRDD: RDD[(sessionId,iterable[UserVisitAction])]
    val sessionId2GroupActionRDD = sessionId2ActionRDD.groupByKey()
    // 缓存
    sessionId2GroupActionRDD.cache()
    // 获取关于一个session的完整数据，包括用户信息
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2GroupActionRDD)

    //定义累加器,累加器中存储的是不同步长，时长的session个数
    val sessionAccumulator = new SessionAccumulator
    //注册累加器
    sparkSession.sparkContext.register(sessionAccumulator)

    //所有符合过滤条件的数据组成的RDD
    // sessionId2FilteredRDD:RDD[(sessionId,fullInfo)]
    val sessionId2FilteredRDD = getSessionFilteredRdd(taskParam, sessionId2FullInfoRDD,sessionAccumulator)
    //执行一个action, execute an action
    sessionId2FilteredRDD.foreach(println)


    //需求一,获取各个范围的session的时长和 步长的函数
    getSessionRatio(sparkSession,taskUUID,sessionAccumulator.value)
    //需求二，session的随机抽取
    sessionRandomExtract(sparkSession,taskUUID,sessionId2FilteredRDD)

    //需求三，Top10 热门商品类别统计
    //sessionId2ActionRDD: RDD[(sessionId,action)]
    //sessionId2FilterRDD: RDD[(sessionId,fullInfo)]
    //将上述 两个RDD进行join 获取过滤后的sessionIdFilterActionRDD
    val sessionIdFilterActionRDD = sessionId2ActionRDD.join(sessionId2FilteredRDD).map {
      case (sessionId, (action, fullInfo)) => {
        (sessionId, action)
      }
    }
    // 需求3具体的实践方法
    // top10CategoryArray:Array((sortKey,countInfo))
    val top10CategoryArray = top10PopularCategories(sparkSession, taskUUID, sessionIdFilterActionRDD)
    // 需求4：Top10热门类别的Top10活跃session统计
    // sessionIdFilterActionRDD:RDD[(sessionId,action)]
    top10ActiveSession(sparkSession,taskUUID,sessionIdFilterActionRDD,top10CategoryArray)
  }

  def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionIdFilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)])={
    // 进行map,得到top10的所有Category
    val cidArray = top10CategoryArray.map {
      case (sortKey, countInfo) => {
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constant.FIELD_CATEGORY_ID).toLong
        cid
      }
    }
    // 进行filter,所有符合过滤条件的，并且点击过Top10热门品类的
    val session2ActionRDD = sessionIdFilterActionRDD.filter {
      case (sessionId, action) => {
        cidArray.contains(action.click_category_id)
      }
    }
    // groupByKey 进行聚合操作
    val sessionId2GroupRDD = session2ActionRDD.groupByKey()
    // 进行flatMap
    val categoryId2SessionCountRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) => {
        // 建立一个map记录一个session对于它所有点击过的品类点击次数
        val categoryCountMap = new mutable.HashMap[Long, Long]()

        for (action <- iterableAction) {
          val cid = action.click_category_id
          if (!categoryCountMap.contains(cid)) {
            categoryCountMap += (cid -> 0)
          }
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }
        // 记录了一个session对于它所有点击过的品类的点击次数
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
      }
    }
    // categoryId2GroupRDD:RDD[(cid,iterableSessionCount)]
    // 每一条数据是一个categoryId和所有点击过它的session 及点击次数
    val categoryId2GroupRDD = categoryId2SessionCountRDD.groupByKey()
    // 获取每个category的 top10 active的session
    // top10SessionRDD:RDD[Top10Session]
    val top10SessionRDD = categoryId2GroupRDD.flatMap {
      case (cid, iterableSessionCount) => {
        // true: item1 放在前面
        // false: item2 放在前面
        val sortList = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)
        //对获取的数据进行map 并且封装
        val top10Session = sortList.map {
          case item => {
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
          }
        }
        top10Session
      }
    }
    //存入数据库中
    //写入数据库
    import  sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constant.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constant.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constant.JDBC_PASSWORD))
      .option("dbtable","session_stat_top10session")
      .mode(SaveMode.Append)
      .save()
  }


  def getClickCount(sessionIdFilterActionRDD: RDD[(String, UserVisitAction)])= {
    // 通过对数据进行过滤
    val clickFilterRDD = sessionIdFilterActionRDD.filter(item => item._2.click_category_id != -1)
    val clickNumRDD = clickFilterRDD.map { case (sessionId, action) => (action.click_category_id, 1L) }
    clickNumRDD.reduceByKey(_+_)
  }

  def getOrderCount(sessionIdFilterActionRDD: RDD[(String, UserVisitAction)])= {
    val orderFilterRDD = sessionIdFilterActionRDD.filter(item => item._2.order_category_ids != null)
    val orderNumRDD = orderFilterRDD.flatMap {
      case (sessionId, action) => {
        action.order_category_ids.split(",").map( item => (item.toLong,1L))
      }
    }
    orderNumRDD.reduceByKey(_+_)
  }

  def getPayCount(sessionIdFilterActionRDD: RDD[(String, UserVisitAction)])= {
    val payFilterRDD = sessionIdFilterActionRDD.filter(item => item._2.pay_category_ids != null)
    val payNumRDD = payFilterRDD.flatMap {
      case (sessionId, action) => {
        action.pay_category_ids.split(",").map( item => (item.toLong,1L))
      }
    }
    payNumRDD.reduceByKey(_+_)
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    // 将cid2CidRDD 与点击统计cid2ClickCountRDD进行leftOutJoin
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) => {
        // 如果option存在值，说明双方都有这个key值，没有则赋值0
        val clickCount = if (option.isDefined) option.get else 0
        //拼接成字符串
        val aggrCount = Constant.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constant.FIELD_CLICK_COUNT + "=" + clickCount
        (cid, aggrCount)
      }
    }
    //将 cid2ClickInfoRDD 与 cid2OrderCountRDD 进行 leftOutJoin
    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) => {
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" + Constant.FIELD_ORDER_COUNT + "=" + orderCount
        (cid, aggrInfo)
      }
    }
    //将cid2OrderInfoRDD 与 cid2PayCountRDD 进行 leftOutJoin
    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) => {
        val payCount = if (option.isDefined) option.get else 0
        val payInfo = orderInfo + "|" + Constant.FIELD_PAY_COUNT + "=" + payCount
        (cid, payInfo)
      }
    }
    cid2PayInfoRDD
  }

  def top10PopularCategories(sparkSession: SparkSession, taskUUID: String, sessionIdFilterActionRDD:RDD[(String,UserVisitAction)]) = {
    // 获取所有有下单、点击、付款行为的action, 并且获取其行为的CategoryId
    // cid2CidRDD:RDD[(categoryId,categoryId)]
    var cid2CidRDD = sessionIdFilterActionRDD.flatMap {
      case (sessionId, action) => {
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()
        // 点击行为
        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          //下单行为，下单可能一次下多个单
          for (orderId <- action.order_category_ids.split(",")) {
            categoryBuffer += ((orderId.toInt, orderId.toInt))
          }
        } else if (action.pay_category_ids != null) {
          //付款行为
          for (payCid <- action.pay_category_ids.split(",")) {
            categoryBuffer += ((payCid.toInt, payCid.toInt))
          }
        }
        categoryBuffer
      }
    }
    // 去重，因为一次action可能同时包含点击，下单，付款等行为
    cid2CidRDD=cid2CidRDD.distinct()

    // 统计品类的点击次数
    val cid2ClickCountRDD = getClickCount(sessionIdFilterActionRDD)
    //统计品类的订单次数
    val cid2OrderCountRDD = getOrderCount(sessionIdFilterActionRDD)
    //统计品类的付款次数
    val cid2PayCountRDD = getPayCount(sessionIdFilterActionRDD)
    //cid2PayCountRDD.foreach(println)
    // 将上述数据进行汇总
    // (93,categoryid=93|clickCount=82|orderCount=79|payCount=80)
    val cid2FullInfoRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)
    //cid2FullInfoRDD.foreach(println)

    //自定义二次排序key,将依据点击、订单、付款进行排序
    val sortKey2FullCountRDD = cid2FullInfoRDD.map {
      case (cid, countInfo) => {
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constant.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constant.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constant.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, countInfo)
      }
    }
    //倒序排序获取前10个最受欢迎的品类
    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)
    // 转换成RDD后 根据获取相关数据
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) => {
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constant.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount
        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
      }
    }
    //写入数据库
    import  sparkSession.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constant.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constant.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constant.JDBC_PASSWORD))
      .option("dbtable","session_stat_top10category")
      .mode(SaveMode.Append)
      .save()

    top10CategoryArray
  }
  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constant.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constant.PARAM_END_DATE)

    import  sparkSession.implicits._
    val sql="select * from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"'"
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  def getSessionFullInfo(sparkSession: SparkSession, sessionId2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    // 将sessionId2GroupActionRDD 进行map,将每一条关于(sessionId,iterable(Action))的数据转化为(userId,AggrInfo)
    val userId2AggrInfoRDD = sessionId2GroupActionRDD.map {
      case (sessionId, iterableActions) =>
        var userId = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0
        var searchKeywords = new StringBuffer("")
        var clickCategories = new StringBuffer("")
        // 循环遍历该session内的所有action
        for (action <- iterableActions) {
          // 获取用户Id,因为每一个session都是一个用户产生的，所以获取一次就够
          if (userId == -1L) {
            userId = action.user_id
          }
          // 获取每次action的发生时间，来由此获取该session内最早的时间
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          // 获取每次action的发生时间，来由此获取该session内最晚的时间
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }
          // 获取每次action的搜索关键字
          // get the search keywords
          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }
          // 获取每次action的查询类别id
          // get the category id of action
          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }
          // update step number
          // 更新action数量
          stepLength += 1
        }
        //去除标点
        val searchKws = StringUtils.trimComma(searchKeywords.toString)
        val clickCgs = StringUtils.trimComma(clickCategories.toString)
        // 获取session的访问时间，由结束时间减去开始时间
        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        //拼接成一个字符串，每个字段使用 "|" 分隔
        val aggrInfo = Constant.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constant.FIELD_SEARCH_KEYWORDS + "=" + searchKws + "|" +
          Constant.FIELD_CLICK_CATEGORY_IDS + "=" + clickCgs + "|" +
          Constant.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constant.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constant.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, aggrInfo)
    }
    //获取所有用户信息数据
    // get the user info data
    val sql="select * from user_info"
    import  sparkSession.implicits._
    //查询获取所有用户信息
    //userId2InfoRDD:RDD[(userId,item)]
    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    // 将用户信息与session信息进行整合，返回对应的RDD
    // the userId2AggrInfoRDD join userInfoRDD
    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2InfoRDD).map {
      case (userId, (aggrInfo, userInfo)) => {
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city
        val fullInfo = aggrInfo + "|" +
          Constant.FIELD_AGE + "=" + age + "|" +
          Constant.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constant.FIELD_SEX + "=" + sex + "|" +
          Constant.FIELD_CITY + "=" + city
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constant.FIELD_SESSION_ID)
        (sessionId, fullInfo)
      }
    }
    sessionId2FullInfoRDD
  }

  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator) = {
    if(visitLength>=1 && visitLength<=3){
      sessionAccumulator.add(Constant.TIME_PERIOD_1s_3s)
    }else if(visitLength>=4 && visitLength<=6){
      sessionAccumulator.add(Constant.TIME_PERIOD_4s_6s)
    }else if(visitLength>=7 && visitLength<=9){
      sessionAccumulator.add(Constant.TIME_PERIOD_7s_9s)
    }else if(visitLength>=10 && visitLength<=30){
      sessionAccumulator.add(Constant.TIME_PERIOD_10s_30s)
    }else if(visitLength>=30 && visitLength<=60){
      sessionAccumulator.add(Constant.TIME_PERIOD_30s_60s)
    }else if(visitLength>=60 && visitLength<=180){
      sessionAccumulator.add(Constant.TIME_PERIOD_1m_3m)
    }else if(visitLength>=180 && visitLength<=600){
      sessionAccumulator.add(Constant.TIME_PERIOD_3m_10m)
    }else if(visitLength>=600 && visitLength<=1800){
      sessionAccumulator.add(Constant.TIME_PERIOD_10m_30m)
    }else if(visitLength>=1800){
      sessionAccumulator.add(Constant.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator) = {
    if(stepLength>=1 && stepLength <=3){
      sessionAccumulator.add(Constant.STEP_PERIOD_1_3)
    }else if (stepLength>=4 && stepLength<=6){
      sessionAccumulator.add(Constant.STEP_PERIOD_4_6)
    } else if (stepLength>=7 && stepLength<=9){
      sessionAccumulator.add(Constant.STEP_PERIOD_7_9)
    }else if (stepLength>=10 && stepLength<=30){
      sessionAccumulator.add(Constant.STEP_PERIOD_10_30)
    }else if (stepLength>=30&& stepLength<=60){
      sessionAccumulator.add(Constant.STEP_PERIOD_30_60)
    }else if (stepLength>60){
      sessionAccumulator.add(Constant.STEP_PERIOD_60)
    }
  }

  def getSessionFilteredRdd(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)], sessionAccumulator: SessionAccumulator) = {
    /**
     * 将获取到的sessionId2FullInfoRDD根据条件进行过滤
     * Filter the obtained sessionId2FullInfoRDD according to conditions
     */
    // get the filter condition
    // 获取过滤条件
    val startAge=ParamUtils.getParam(taskParam,Constant.PARAM_START_AGE)
    val endAge=ParamUtils.getParam(taskParam,Constant.PARAM_END_AGE)
    val professionals=ParamUtils.getParam(taskParam,Constant.PARAM_PROFESSIONALS)
    val cities=ParamUtils.getParam(taskParam,Constant.PARAM_CITIES)
    val sex=ParamUtils.getParam(taskParam,Constant.PARAM_SEX)
    val keywords=ParamUtils.getParam(taskParam,Constant.PARAM_KEYWORDS)
    val categoryIds=ParamUtils.getParam(taskParam,Constant.PARAM_CATEGORY_IDS)

    // 拼接过滤条件
    //
    var filterInfo=(if(startAge !=null && !startAge.equals("")) Constant.PARAM_START_AGE +"="+startAge+"|" else "")+
      (if(endAge !=null && !endAge.equals("")) Constant.PARAM_END_AGE +"="+endAge+"|" else "")+
      (if(professionals !=null && !professionals.equals("")) Constant.PARAM_PROFESSIONALS +"="+professionals+"|" else "")+
      (if(cities !=null && !cities.equals("")) Constant.PARAM_CITIES +"="+cities+"|" else "")+
      (if(sex !=null && !sex.equals("")) Constant.PARAM_SEX +"="+sex+"|" else "")+
      (if(keywords !=null && !keywords.equals("")) Constant.PARAM_KEYWORDS +"="+keywords+"|" else "")+
      (if(categoryIds !=null && !categoryIds.equals("")) Constant.PARAM_CATEGORY_IDS +"="+categoryIds+"|" else "")


    if (filterInfo.endsWith("|"))
      filterInfo=filterInfo.substring(0,filterInfo.length-1)

    val sessionId2FilteredRDD = sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) => {
        var success = true
        if (!ValidUtils.between(fullInfo, Constant.FIELD_AGE, filterInfo, Constant.PARAM_START_AGE, Constant.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constant.FIELD_PROFESSIONAL, filterInfo, Constant.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constant.FIELD_CITY, filterInfo, Constant.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constant.FIELD_SEX, filterInfo, Constant.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constant.FIELD_SEARCH_KEYWORDS, filterInfo, Constant.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constant.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constant.PARAM_CATEGORY_IDS))
          success = false

        //符合过滤条件的session，累加
        if(success){
          // 累加session的个数
          sessionAccumulator.add(Constant.SESSION_COUNT)
          // 获取时长与步长
          val visitLength=StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constant.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constant.FIELD_STEP_LENGTH).toLong

          // 根据不同的范围获取不同的key值进行累加
          calculateVisitLength(visitLength,sessionAccumulator)
          calculateStepLength(stepLength,sessionAccumulator)
        }
        success
      }
    }
    sessionId2FilteredRDD
  }

  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val  sessionCount=value.getOrElse(Constant.SESSION_COUNT,1).toDouble

    val visitLength_1s_3s=value.getOrElse(Constant.TIME_PERIOD_1s_3s,0)
    val visitLength_4s_6s=value.getOrElse(Constant.TIME_PERIOD_4s_6s,0)
    val visitLength_7s_9s=value.getOrElse(Constant.TIME_PERIOD_7s_9s,0)
    val visitLength_10s_30s=value.getOrElse(Constant.TIME_PERIOD_10s_30s,0)
    val visitLength_30s_60s=value.getOrElse(Constant.TIME_PERIOD_30s_60s,0)
    val visitLength_1m_3m=value.getOrElse(Constant.TIME_PERIOD_1m_3m,0)
    val visitLength_3m_10m=value.getOrElse(Constant.TIME_PERIOD_3m_10m,0)
    val visitLength_10m_30m=value.getOrElse(Constant.TIME_PERIOD_10m_30m,0)
    val visitLength_30m=value.getOrElse(Constant.TIME_PERIOD_30m,0)

    val stepLength_1_3=value.getOrElse(Constant.STEP_PERIOD_1_3,0)
    val stepLength_4_6=value.getOrElse(Constant.STEP_PERIOD_4_6,0)
    val stepLength_7_9=value.getOrElse(Constant.STEP_PERIOD_7_9,0)
    val stepLength_10_30=value.getOrElse(Constant.STEP_PERIOD_10_30,0)
    val stepLength_30_60=value.getOrElse(Constant.STEP_PERIOD_30_60,0)
    val stepLength_60=value.getOrElse(Constant.STEP_PERIOD_60,0)

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visitLength_1s_3s / sessionCount, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visitLength_4s_6s / sessionCount, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visitLength_7s_9s / sessionCount, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visitLength_10s_30s / sessionCount, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visitLength_30s_60s / sessionCount, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visitLength_1m_3m / sessionCount, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visitLength_3m_10m / sessionCount, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visitLength_10m_30m / sessionCount, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visitLength_30m / sessionCount, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(stepLength_1_3 / sessionCount, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(stepLength_4_6 / sessionCount, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(stepLength_7_9 / sessionCount, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(stepLength_10_30 / sessionCount, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(stepLength_30_60 / sessionCount, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(stepLength_60 / sessionCount, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = SessionAggrStat(taskUUID,
      sessionCount.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionAggrStatRDD = sparkSession.sparkContext.makeRDD(Array(sessionAggrStat))
    import  sparkSession.implicits._
    // 写入到Mysql数据库中
    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constant.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constant.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constant.JDBC_PASSWORD))
      .option("dbtable","session_stat_ratio")
      .mode(SaveMode.Append)
      .save()
  }

  def generateDateRandomIndexList(extractPerDay: Int,
                              dateSessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                                  hourListMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    // 针对每个小时的次数
    for((hour,count)<-hourCountMap) {
      //一个小时要抽取的session数量=(这个小时的session数量/这一天的session数量）* 这一天要抽取的session数量
      var hourExtractCount = ((count / dateSessionCount.toDouble) * extractPerDay).toInt
      // 避免一个小时的数量超过这个小时的总数
      if(hourExtractCount>count){
        hourExtractCount=count.toInt
      }
      val random = new Random()
      //存储数据
      hourListMap.get(hour) match{
          //当前map中没有该小时的数据
        case None =>{
          //新创建一个列表，并且赋值
          hourListMap(hour)=new ListBuffer[Int]
          // 根据每个小时要抽取的个数，随机选择
          for( i <- 0 until hourExtractCount){
            //随机生成数字
            var index = random.nextInt(count.toInt)
            //判断是否已经存在，如果存在那就再随机生成
            while(hourListMap(hour).contains(index)){
              index=random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        }
          //当前map中已经有相对应的数据
        case Some(list) =>{
          for( i <- 0 until hourExtractCount){
            //随机生成数字
            var index = random.nextInt(count.toInt)
            //判断是否已经存在，如果存在那就再随机生成
            while(hourListMap(hour).contains(index)){
              index=random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        }
      }
    }

  }

  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FilteredRDD: RDD[(String, String)]): Unit = {
    // 对RDD 进行key值的转化
    // dateHour2fullInfo:RDD[(dateHour,fullInfo)]
    val dateHour2fullInfo = sessionId2FilteredRDD.map {
      case (sessionId, fullInfo) => {
        // 获取该session的开始时间
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constant.FIELD_START_TIME)
        // 得到对应的action访问时间 转化格式为 yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
      }
    }
    // 进行相同key值得value个数统计,得到每个小时的session个数
    // hourCountMap:Map[(dateHour,count_number)]
    val hourCountMap = dateHour2fullInfo.countByKey()

    //创建一个Map用于数据存储,存储每天里的各个小时的访问个数
    // dateHourCountMap:Map[(date,Map[(hour,count_number)])]
    val dateHourCountMap=new mutable.HashMap[String,mutable.HashMap[String,Long]]()
    // 对上述得到hourCount进行遍历
    for((dateHour,count)<-hourCountMap){
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      //对key值为date的dateHourCountMap进行遍历
      dateHourCountMap.get(date) match {
        // 如果当前没有key值，新创建key,value
        case None => dateHourCountMap(date)=new mutable.HashMap[String,Long]()
          dateHourCountMap(date)+=(hour->count)
        //如果已经存在key值就累加
        case some => dateHourCountMap(date)+=(hour->count)
      }
    }

    //计算平均每天要获取多少条数据，dateHourCountMap.size 为天数
    val extractPerDay = 100 / dateHourCountMap.size
    // 创建一个变量，计算每天中每个小时要提取的数据序号,和上诉不同
    val dateHourExtractIndexListMap=new mutable.HashMap[String,mutable.HashMap[String,ListBuffer[Int]]]()
    for((date,hourCountMap) <- dateHourCountMap){
      //每一天session的个数
      val dateSessionCount = hourCountMap.values.sum
      //存储进map中
      dateHourExtractIndexListMap.get(date) match{
          //如果没有当天的数据
        case None =>{
          dateHourExtractIndexListMap(date)=new mutable.HashMap[String,ListBuffer[Int]]()
          generateDateRandomIndexList(extractPerDay,dateSessionCount,hourCountMap,dateHourExtractIndexListMap(date))
        }
          //存在当天的数据就加入
        case Some(x) => {
          generateDateRandomIndexList(extractPerDay,dateSessionCount,hourCountMap,dateHourExtractIndexListMap(date))
        }
      }

    }
    //生成广播变量，因为这是所有task共同维护的,这种不需要使用spark的一般都在Driver中,如果不进行广播，各个executor会再执行一次
    val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)
    // dateHour2GroupRDD:RDD[(dateHour,iterable(fullInfo)]
    // 根据key值进行聚合
    val dateHour2GroupRDD = dateHour2fullInfo.groupByKey()
    //获取抽取到的相对应的RDD
    // extractSessionRDD：RDD[SessionRandomExtract]
    val extractSessionRDD = dateHour2GroupRDD.flatMap {
      case (dateHour, iterableFullInfo) => {
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        //获取当天并且每隔小时要抽取的序号
        val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)
        // 创建一个数组，用来存储随机抽取的数据封装类
        val extractsessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()
        //序号
        var index = 0
        for (fullInfo <- iterableFullInfo) {
          //如果要抽取的序号中包括当前序号
          if (extractList.contains(index)) {
            // 获取关于fullInfo中相关的数据
            val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constant.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constant.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constant.FIELD_SEARCH_KEYWORDS)
            val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constant.FIELD_CLICK_CATEGORY_IDS)
            //封装成类
            val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)
            extractsessionArrayBuffer += extractSession
          }
          index += 1
        }
        extractsessionArrayBuffer
      }
    }

    //写入对应的数据库中
    import  sparkSession.implicits._
    extractSessionRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constant.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constant.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constant.JDBC_PASSWORD))
      .option("dbtable","session_random_extract")
      .mode(SaveMode.Append)
      .save()
  }

}
