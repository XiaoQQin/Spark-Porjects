/**
 * 需求一中最终的数据存入到Mysql
 * @param taskid
 * @param session_count
 * @param visit_length_1s_3s_ratio
 * @param visit_length_4s_6s_ratio
 * @param visit_length_7s_9s_ratio
 * @param visit_length_10s_30s_ratio
 * @param visit_length_30s_60s_ratio
 * @param visit_length_1m_3m_ratio
 * @param visit_length_3m_10m_ratio
 * @param visit_length_10m_30m_ratio
 * @param visit_length_30m_ratio
 * @param step_length_1_3_ratio
 * @param step_length_4_6_ratio
 * @param step_length_7_9_ratio
 * @param step_length_10_30_ratio
 * @param step_length_30_60_ratio
 * @param step_length_60_ratio
 */
case class SessionAggrStat(taskid: String,
                           session_count: Long,
                           visit_length_1s_3s_ratio: Double,
                           visit_length_4s_6s_ratio: Double,
                           visit_length_7s_9s_ratio: Double,
                           visit_length_10s_30s_ratio: Double,
                           visit_length_30s_60s_ratio: Double,
                           visit_length_1m_3m_ratio: Double,
                           visit_length_3m_10m_ratio: Double,
                           visit_length_10m_30m_ratio: Double,
                           visit_length_30m_ratio: Double,
                           step_length_1_3_ratio: Double,
                           step_length_4_6_ratio: Double,
                           step_length_7_9_ratio: Double,
                           step_length_10_30_ratio: Double,
                           step_length_30_60_ratio: Double,
                           step_length_60_ratio: Double
                          )


/**
 * 需求二、Session随机抽取表
 *
 * @param taskid             当前计算批次的ID
 * @param sessionid          抽取的Session的ID
 * @param startTime          Session的开始时间
 * @param searchKeywords     Session的查询字段
 * @param clickCategoryIds   Session点击的类别id集合
 */
case class SessionRandomExtract(taskid:String,
                                sessionid:String,
                                startTime:String,
                                searchKeywords:String,
                                clickCategoryIds:String)


/** 需求3 中
 * 品类Top10表
 * @param taskid
 * @param categoryid
 * @param clickCount
 * @param orderCount
 * @param payCount
 */
case class Top10Category(taskid:String,
                         categoryid:Long,
                         clickCount:Long,
                         orderCount:Long,
                         payCount:Long)

/** 需求4中每个
 * Top10 Session
 * @param taskid
 * @param categoryid
 * @param sessionid
 * @param clickCount
 */
case class Top10Session(taskid:String,
                        categoryid:Long,
                        sessionid:String,
                        clickCount:Long)

