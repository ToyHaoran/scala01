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

        //在这里处理一下，不区分大小写
        val low = key.toLowerCase
        val value = properties.getProperty(low)
        if (null == value) {
            properties.getProperty(low.toUpperCase)
        } else {
            value
        }
    }
}

object PropUtil {
    /**
      * 获取工具类
      * @param filePath 配置文件路径
      */
    def apply(filePath: String): PropUtil = {
        //可以不添加后缀
        if (filePath.endsWith("properties")) {
            new PropUtil(filePath)
        } else {
            new PropUtil(filePath + ".properties")
        }
    }

    /**
      * 用于随时获取获取某个值
      */
    def getValueByKey(key: String, filePath: String = "app.properties"): String = {
        new PropUtil(filePath).getConfig(key)
    }

    /**
      * 测试这个工具类
      */
    def main(args: Array[String]) {
        import utils.BaseUtil._
        //测试读取配置文件
        if (0) {
            val propUtil = PropUtil("localdb")
            val user = propUtil.getConfig("USER")
            val password = propUtil.getConfig("PASSword")
            val url = propUtil.getConfig("URL")
            println(user + " " + password + " " + url)
        }

        //测试随时获取某个值
        if(0){
            val aaa = PropUtil.getValueByKey("aaa")
            println(aaa)
        }
    }
}