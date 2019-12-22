package domain

/***
  * the click count of course one day
  * @param day_courseid rowkey in hbase 20171111_1
  * @param click_count  the click count 20171111_1
  */
case class CourseClickCount(day_courseid:String,click_count:Long)
