package utils

import scala.util.parsing.json.JSON
import BaseUtil._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/13
  * Time: 11:17 
  * Description:
  */
object JsonUtil {
  /*
    怎么往main函数中传入json字符串："{\"in_csrq\":\"2018-12-12\"}"  注意，这是一个参数arg(0)，内部引号需要加转义字符，然后后面加空格写第二个参数。
    参考费控：/usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 27g --executor-memory 16g --executor-cores 3  --num-executors 20  --master yarn-cluster --class com.dingxin.entrance.EntranceD  --name 费控计算lhr --driver-class-path /usr/local/jar/ojdbc7.jar,/usr/local/jar/mysql-connector-java-8.0.11.jar --jars /usr/local/jar/ojdbc7.jar,/usr/local/jar/mysql-connector-java-8.0.11.jar,/usr/local/jar/sparkts-0.4.0.jar,/usr/local/jar/json.jar,/usr/local/jar/spark-hbase-connector-2.2.0-1.1.2-3.4.6.jar,/usr/local/jar/lihaoran/dl.jar,/usr/local/jar/lihaoran/df.jar,/usr/local/jar/lihaoran/DataInit.jar  /usr/local/jar/lihaoran/feikong.jar  "{\"in_csrq\":\"2018-12-12\"}"
   */
  def main(args: Array[String]) {
    if (0) {
      //val jsonStr = """{"key1":"Ricky", "key2":"21"}"""
      println("JSON字符串解析===========")
      val jsonStr = args(0)
      println(jsonStr)
      val jsonValue = JSON.parseFull(jsonStr)
      println(jsonValue.toString)
      val map: Map[String, Any] = jsonValue.get.asInstanceOf[Map[String, Any]]
      val key1 = map.get("key1").get.asInstanceOf[String]
      val key2 = map.get("key2").get.asInstanceOf[String]
      print(key1 + key2)
    }

    if (0) {
      println("多级JSON字符串解析=============")
      val jsonStr = """{"username":"Ricky", "attribute":{"age":21, "weight": 60}}"""
      val jsonValue = JSON.parseFull(jsonStr)
      println(jsonValue.toString)
      val map: Map[String, Any] = jsonValue.get.asInstanceOf[Map[String, Any]]
      val username = map.get("username").get.asInstanceOf[String]
      val attribute = map.get("attribute").get.asInstanceOf[Map[String, Double]]
      val age = attribute.get("age").get
      val weight = attribute.get("weight").get
      print(username + " " + age + " " + weight)

    }
  }
}