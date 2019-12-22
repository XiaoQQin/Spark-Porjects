package dao

import domain.{CourseClickCount, CourseSearchClickCount}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import utils.HBaseUtils

import scala.collection.mutable.ListBuffer

object CourseSearchClickCountDAO {
  private val tableName = "course_search_clickcount"
  private val cf="info"
  private val qualifer = "click_count"


  /***
    * keep data in hbase
    * @param list CourseClickCount
    */
  def save(list:ListBuffer[CourseSearchClickCount])={
//    val hTable:HTable = new HTable(conf, tableName)
//    for(l <- list){
//      hTable.incrementColumnValue(Bytes.toBytes(l.day_courseid),
//        Bytes.toBytes(cf),
//        Bytes.toBytes(qualifer),l.click_count)
//    }

    val table: HTable = HBaseUtils.getInstance().getTable(tableName)
    for(l <- list){
        table.incrementColumnValue(Bytes.toBytes(l.day_search_courseid),
          Bytes.toBytes(cf),
          Bytes.toBytes(qualifer),
          l.click_count)
    }

  }

  /***
    *  accord the rowkey get the data
    * @param day_search_courseid
    */
  def count(day_search_courseid:String)={
    val table: HTable = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_search_courseid))
    val value: Array[Byte] = table.get(get).getValue(Bytes.toBytes(cf),Bytes.toBytes(qualifer))

    if (value==null){
      0L
    }else{
      Bytes.toLong(value)
    }

  }

  def main(args: Array[String]): Unit = {
    val list=new ListBuffer[CourseSearchClickCount];
    list.append(CourseSearchClickCount("20191111_8",23))
    list.append(CourseSearchClickCount("20191111_9",4))
    list.append(CourseSearchClickCount("20191111_1",100))


    save(list)
    //println(count("20191111_8")+" : "+count("20191111_9")+" : "+count("20191111_1"))
  }
}
