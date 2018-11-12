package utils

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/9
  * Time: 14:27 
  * Description:
  */
object ReadDataBase {
    def main(args: Array[String]) {
        val spark = ConnectUtil.getLocalSpark
        import spark.implicits._

        val url2 = getMysqlUrl("localhost","taotao","3307")
        val user = "root2"
        val password = "root"
        val dataBaseProps = new java.util.Properties()
        dataBaseProps.setProperty("user", user)
        dataBaseProps.setProperty("password", password)
        dataBaseProps.setProperty("fetchsize", "1000") //批量读
        dataBaseProps.setProperty("batchsize", "5000") //批量写

        // 演示
        // 注意第二个参数是TABLE_NAME，相当于子查询
        // val jldxx = spark.read.jdbc(url, "(select * from hs_jldxx where rownum<=1000)", dataBaseProps)
        // val temp = spark.read.jdbc(url2, "(select * from student) as st", dataBaseProps)
        // jldxx.write.mode(SaveMode.Overwrite).parquet("src/sparkdemo/testfile/temp")


        val temp = spark.read.jdbc(url2, "(select * from student) as st", dataBaseProps)
        temp.show()
        temp.printSchema()

        //可以用来生成测试数据
        temp.repartition(2).write.mode(SaveMode.Overwrite).parquet("src/sparkdemo/testfile/temp")

        val temp02 = spark.read.parquet("src/sparkdemo/testfile/temp")
        temp02.show()
        temp02.printSchema()
    }


    /**
      * 得到Oracle的URL
      */
    def getOracleUrl(host: String, serviceName: String, port: String = "1521"): String = {
        //需要有读取Oracle的jar包
        //不知道为什么，简写版的连不上。
        s"jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = $host)(PORT = $port)))(CONNECT_DATA=(SERVER = DEDICATED)(SERVICE_NAME = $serviceName)))"
    }

    /**
      * 得到Mysql的URl
      */
    def getMysqlUrl(host: String, dbName: String, port: String = "3306"): String = {
        s"jdbc:mysql://$host:$port/$dbName"
    }
}