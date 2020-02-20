package commons.model

/**
 *
 * user visit data
 *
 * @param date               the date of user clicked
 * @param user_id            ID
 * @param session_id         Session ID
 * @param page_id            the page ID of clicked
 * @param action_time        the time in one day
 * @param search_keyword     search keyWord
 * @param click_category_id  category ID of one product clicked
 * @param click_product_id   product ID
 * @param order_category_ids category ID set in one order
 * @param order_product_ids  product ID set in one order
 * @param pay_category_ids   category ID set in one pay
 * @param pay_product_ids    product ID set in one pay
 * @param city_id            city ID
 */
case class UserVisitAction(
                          date:String,
                          user_id:Long,
                          session_id:String,
                          page_id:Long,
                          action_time:String,
                          search_keyword:String,
                          click_category_id: Long,
                          click_product_id: Long,
                          order_category_ids: String,
                          order_product_ids: String,
                          pay_category_ids: String,
                          pay_product_ids: String,
                          city_id: Long
                          )


case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    city: String,
                    sex: String)


/**
 * Product table
 *
 * @param product_id   ID
 * @param product_name name
 * @param extend_info  other info
 */
case class ProductInfo(product_id: Long,
                       product_name: String,
                       extend_info: String
                      )
case class SessionAggrStat()