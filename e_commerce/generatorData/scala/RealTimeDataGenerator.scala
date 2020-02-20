import java.util.Properties

import commons.conf.ConfigurationManager
import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object RealTimeDataGenerator {

  def createKafkaProducer(broker: String)={
    //创建连接Kafka的参数
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 创建Kafka的生产者
    new KafkaProducer[String,String](prop)
  }

  def generateData()={
    val array =ArrayBuffer[String]()
    val random = new Random()

    // 每次产生50条数据
    for( i <- 0 to 50){
      // 获取具体的时间戳
      val timestamp = System.currentTimeMillis()
      val province = random.nextInt(10)
      val city = province
      val adid = random.nextInt(20)
      val userid = random.nextInt(100)
      // 拼接成字符串，加入数组中
      array += timestamp + " " + province + " " + city + " " + userid + " " + adid
    }
    array.toArray
  }

  def main(args: Array[String]): Unit = {
    // 获取kafka的主机地址
    val broker = ConfigurationManager.config.getString("kafka.broker.list")
    // 获取kafka用来传递数据的主题
    val topic = ConfigurationManager.config.getString("kafka.topics")
    //创建Kafka生产者
    val kafkaProducer = createKafkaProducer(broker)

    // 新建一个死循环，产生数据
    while (true){
      for(item <- generateData()){
        //发送数据
        kafkaProducer.send(new ProducerRecord[String,String](topic,item))
      }
      //每隔5秒发送一次数据
      Thread.sleep(500)
    }
  }
}
