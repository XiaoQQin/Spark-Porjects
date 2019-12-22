package dao

import domain.CourseClickCount
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import utils.HBaseUtils

object CourseClickCountDAO {
  private val tableName = "course_clickcount"
  private val cf="info"
  private val qualifer = "click_count"


  /***
    * keep data in hbase
    * @param list CourseClickCount
    */
  def save(list:ListBuffer[CourseClickCount])={
//    val hTable:HTable = new HTable(conf, tableName)
//    for(l <- list){
//      hTable.incrementColumnValue(Bytes.toBytes(l.day_courseid),
//        Bytes.toBytes(cf),
//        Bytes.toBytes(qualifer),l.click_count)
//    }

    val table: HTable = HBaseUtils.getInstance().getTable(tableName)
    for(l <- list){
        table.incrementColumnValue(Bytes.toBytes(l.day_courseid),
          Bytes.toBytes(cf),
          Bytes.toBytes(qualifer),
          l.click_count)
    }

  }

  /***
    *  accord the rowkey get the data
    * @param day_courseid
    */
  def count(day_courseid:String)={
    val table: HTable = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_courseid))
    val value: Array[Byte] = table.get(get).getValue(Bytes.toBytes(cf),Bytes.toBytes(qualifer))

    if (value==null){
      0L
    }else{
      Bytes.toLong(value)
    }

  }

  def main(args: Array[String]): Unit = {
    val list=new ListBuffer[CourseClickCount];
    list.append(CourseClickCount("20191111_8",23))
    list.append(CourseClickCount("20191111_9",4))
    list.append(CourseClickCount("20191111_1",100))


    save(list)
    //println(count("20191111_8")+" : "+count("20191111_9")+" : "+count("20191111_1"))
  }
}
