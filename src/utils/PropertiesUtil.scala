package utils

import java.io.IOException
import java.nio.file.NoSuchFileException
import java.util.Properties

/**
  * Created with IntelliJ IDEA.
  * User: jack 
  * Date: 2018/8/31
  * Time: 14:02 
  * Description:
  */
class PropertiesUtil(filePath: String = PropertiesUtil.defaultPropertiesPatch) {

  private var properties: Properties = new Properties()

  /**
    * 私有加载配置文件函数，不对外开放
 *
    * @return
    */
  private def loadProperties(): Properties = {
    try{
      val propertiesStream = this.getClass.getClassLoader.getResourceAsStream(filePath)
      properties.load(propertiesStream)
    } catch {
      case _: NullPointerException =>
        try{

        } catch {
          case e: IOException =>
            throw new NoSuchFileException(s"/$filePath doses not exists")
        }
    }
    properties
  }

  /**
    * 获取配置项
 *
    * @param key
    * @return
    */
  def getConfig(key: String): String = getProperty(key)

  /**
    * 以Option格式返回配置项
 *
    * @param key
    * @return
    */
  def get(key: String): Option[String] = {
    Option(getConfig(key))
  }

  /**
    * 获取配置项，如果配置项存在则返回配置项对应的内容如果配置项不存在则返回默认值
 *
    * @param key
    * @param default
    * @return
    */
  def getOrDefault(key: String, default: String): String =
    get(key).getOrElse(default)

  /**
    * 私有函数不对外部开放
 *
    * @param key
    * @return
    */
  private def getProperty(key: String): String = {
    if(properties.isEmpty){
      properties = loadProperties()
    }
    properties.getProperty(key)
  }

}

object PropertiesUtil{

  var defaultPropertiesPatch = "app.properties"
  lazy val defaultPropertiesUtil = new PropertiesUtil(defaultPropertiesPatch)

  def apply(filePath:String = defaultPropertiesPatch): PropertiesUtil ={
    new PropertiesUtil(filePath)
  }

  def getConfig(key:String): String = defaultPropertiesUtil.getConfig(key)

}
