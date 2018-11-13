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
    def main(args: Array[String]) {
        if(0){
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

        if(0){
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