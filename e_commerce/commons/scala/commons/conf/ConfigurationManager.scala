package commons.conf

import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

/**
 * the class of conf util
 */
object ConfigurationManager {

  private val params=new Parameters()
  // FileBasedConfigurationBuilder:产生一个传入的类的实例对象
  // FileBasedConfiguration:融合FileBased与Configuration的接口
  // PropertiesConfiguration:从一个或者多个文件读取配置的标准配置加载器
  // configure():通过params实例初始化配置生成器
  // 向FileBasedConfigurationBuilder()中传入一个标准配置加载器类，生成一个加载器类的实例对象，然后通过params参数对其初始化
  private val builder: FileBasedConfigurationBuilder[FileBasedConfiguration] = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(params.properties().setFileName("commerce.properties"))

  val config: FileBasedConfiguration = builder.getConfiguration

}

