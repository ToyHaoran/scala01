package utils

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created with IntelliJ IDEA.
  * User: jack 
  * Date: 2018/9/6
  * Time: 9:37 
  * Description:
  */

class DataReadUtil private(propertiesUtil: PropertiesUtil, dbIndex: Int, dbType: String, spark: SparkSession) {

    /**
    * 读取jdbc数据
 *
    * @param dbIndex
    * @param dbType
    * @param table
    * @return
    */
    def jdbc(dbIndex: Int, dbType: String, table: String): DataFrame = {
        val url: String = "DB" + dbIndex + "." + dbType + ".URL"
        val properties: Properties = createProperties(dbIndex, dbType)
        require(url != null && !"".equals(url), "DataBase URL Required")
        spark.read.jdbc(url, table, properties)
    }

    /**
      * 读取jdbc数据
 *
      * @param table
      * @return
      */
    def jdbc(table: String): DataFrame = {
        val url: String = "DB" + dbIndex + "." + dbType + ".URL"
        val properties: Properties = createProperties(dbIndex, dbType)
        require(url != null && !"".equals(url), "DataBase URL Required")
        spark.read.jdbc(url, table, properties)
    }

    /**
    * 按照predicates分片读取Oracle数据
 *
    * @param dbIndex
    * @param dbType
    * @param table
    * @param predicates
    * @return
    */
    def jdbc(dbIndex: Int, dbType: String, table: String, predicates: Array[String]): DataFrame = {
        val u: String = "DB" + dbIndex + "." + dbType + ".URL"
        val properties: Properties = createProperties(dbIndex, dbType)
        val url: String = propertiesUtil.getConfig(u)
        require(url != null && !"".equals(url), "DataBase URL Required")
        spark.read.jdbc(url, table, predicates, properties)
    }

    /**
      * 按照predicates分片读取Oracle数据
 *
      * @param table
      * @param predicates
      * @return
      */
    def jdbc(table: String, predicates: Array[String]): DataFrame = {
        val u: String = "DB" + dbIndex + "." + dbType + ".URL"
        val properties: Properties = createProperties(dbIndex, dbType)
        val url: String = propertiesUtil.getConfig(u)
        require(url != null && !"".equals(url), "DataBase URL Required")
        spark.read.jdbc(url, table, predicates, properties)
    }

    /**
    * 根据配置文件路径读取Parquet
 *
    * @param key
    * @return
    */
    def parquet(key: String): DataFrame = {
        val path: String = propertiesUtil.getConfig(key)
        spark.read.parquet(key)
    }

    /**
    * 构建数据库连接参数中的属性对象
 *
    * @param dbIndex
    * @param dbType
    * @return
    */
    private def createProperties(dbIndex: Int, dbType: String): Properties = {
        val u: String = "DB" + dbIndex + "." + dbType + ".USER"
        val p: String = "DB" + dbIndex + "." + dbType + ".PASSWORD"
        val d: String = "DB" + dbIndex + "." + dbType + ".DRIVER"
        val user: String = propertiesUtil.getConfig(u)
        val pass: String = propertiesUtil.getConfig(p)
        val driver: String = propertiesUtil.getConfig(d)
        require(user != null && !"".equals(user), "DataBase USER Required")
        require(pass != null && !"".equals(pass), "DataBase PASSWORD Required")
        require(driver != null && !"".equals(driver), "DataBase DRIVER Required")
        val properties: Properties = new Properties
        properties.setProperty("user", user)
        properties.setProperty("password", pass)
        properties.setProperty("driver", driver)
        properties
    }

}

object DataReadUtil{
    def getInstance(propertiesUtil: PropertiesUtil, dbIndex: Int, dbType: String, spark: SparkSession): DataReadUtil = {
        new DataReadUtil(propertiesUtil, dbIndex, dbType, spark)
    }
}

//case class ParquetPartition(whereClause: String, idx: Int) extends Partition {
//  override def index: Int = idx
//}
