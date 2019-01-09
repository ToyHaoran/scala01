package utils

import java.io.InputStreamReader
import java.util.Properties

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/23
  * Time: 15:16 
  * Description:
  */
class PropUtil(filePath: String) {

  private var properties: Properties = new Properties()

  /**
    * 得到Properties
    */
  def getProperties(): Properties = {
    val propertiesStream = new InputStreamReader(this.getClass.getClassLoader.getResourceAsStream(filePath), "UTF-8")
    properties.load(propertiesStream)
    properties
  }

  /**
    * 获取配置项，不区分大小写
    */
  def getConfig(key: String): String = {
    //第一次初始化
    if (properties.isEmpty) {
      properties = getProperties()
    }

    //在这里处理一下，不区分大小写，（虽然全部是大写）
    var value = properties.getProperty(key)

    val low = key.toLowerCase
    val upper = key.toUpperCase
    if (null == value) {
      value = properties.getProperty(low)
      if (null == value) {
        value = properties.getProperty(upper)
      }
    }
    value
  }
}

object PropUtil {
  /**
    * 获取工具类
    *
    * @param filePath 配置文件路径，可以不添加后缀
    */
  def apply(filePath: String): PropUtil = {
    if (filePath.endsWith(".properties")) {
      new PropUtil(filePath)
    } else {
      new PropUtil(filePath + ".properties")
    }
  }

  /**
    * 用于随时获取获取某个值
    */
  def getValueByKey(key: String, filePath: String = "app.properties"): String = {
    PropUtil(filePath).getConfig(key)
  }

  /**
    * 得到表的主键。
    *
    * @return 主键数组
    */
  def getPrimaryKeys(table: String): Array[String] = {
    val v = getValueByKey(table, "tableprimarykey.properties")
    if (null != v || !"".equals(v)) v.split(",") else Array()
  }

  /**
    * 测试这个工具类
    */
  def main(args: Array[String]) {
    import utils.BaseUtil._
    //测试读取配置文件
    if (1) {
      val propUtil = PropUtil("localdb")
      val user = propUtil.getConfig("USER")
      val password = propUtil.getConfig("PASSword")
      val url = propUtil.getConfig("URL")
      println(user + " " + password + " " + url)

      val properties = propUtil.getProperties()
      //打印配置文件
      properties.list(System.out)
      println(properties.propertyNames())
      //所有的key，不能改变大小写，所以规定配置项全部大写。
      println(properties.stringPropertyNames())

    }

    //测试随时获取某个值,注意配置文件中的key必须全部大写或者全部小写，不能大小写混合，要不然读取不到。
    if (1) {
      val aaa = PropUtil.getValueByKey("XIAOXIE", "app")
      println(aaa)
    }

  }
}