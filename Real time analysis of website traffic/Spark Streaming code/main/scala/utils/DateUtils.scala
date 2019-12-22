package utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {
  private val YYYYMMDDHHMMSS_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  private val TARGE_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")


  def getTime(time:String)={
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time:String)={
    TARGE_FORMAT.format(new Date(getTime(time)))
  }

}
