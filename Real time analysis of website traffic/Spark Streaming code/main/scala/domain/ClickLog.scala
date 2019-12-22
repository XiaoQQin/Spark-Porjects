package domain

/***
  *
  * @param ip IP address of the access log
  * @param time time of access log
  * @param courseId access course ID
  * @param statusCode the status of access
  * @param referer
  */

case class ClickLog(ip:String,time:String,courseId:Int,statusCode:Int,referer:String)
