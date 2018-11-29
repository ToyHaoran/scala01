package utils.phoenix

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import utils.ConnectUtil
import utils.BaseUtil._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/14
  * Time: 8:40 
  * Description:Scala使用spark操作phoenix
  */
object TestSpark01 {
    def main(args: Array[String]) {
        //读写数据前提：Hbase中必须存在phoenix相关的jar包，IDEA中必须有相关jar包
        //本文件需要jar包：phoenix-4.10.0.2.6.0.3-8-client.jar
        val spark = ConnectUtil.spark

        //———————————————————————读取数据——————————————————————————————
        if(0){
            //读取本地数据
            val df = readFromPhoenix("localhost","(select * from student)",spark)
            df.show()
        }

        if(0){
            //读取集群数据
            val df2 = readFromPhoenix("172.20.32.212", "(select * from bmsd)", spark)
            df2.show()
        }

        //—————————————————————写入数据————————————————————————————————
        if (0) {
            /*
            已有student表
            ID  | NAME  | SEX  | AGE

            创建一个学生表（在shell中创建）：
            create table if NOT EXISTS STUDENT(
                id char(10) not null primary key,
                name char(10),
                sex char(10),
                age integer
            );
            */

            import spark.implicits._
            val df = spark.createDataset(Seq(("1111", "zhangsan", "male", 14), ("1112", "lisi", "female", 15),
                ("1113", "wangwu", "male", 15), ("1114", "zhaoliu", "female", 16)))
                .toDF("ID", "NAME", "SEX", "AGE")
            df.show

            //将df写入对应的表中
            writeToPhoenix(df,"localhost","student")
        }
    }


    /**
      * 将df写入对应的phoenix表中
      *
      * @param df   需要写入的DF
      * @param IP   IP地址如localhost
      * @param table 所要插入的表，可以是子查询
      * @param port 端口号，默认是2181
      */
    def writeToPhoenix(df: DataFrame,IP: String, table: String,port:String = "2181"): Unit = {
        df.write.format("org.apache.phoenix.spark")
            .mode(SaveMode.Overwrite)
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
            .option("zkUrl", "jdbc:phoenix:localhost:2181")
            .option("table", "student")
            .save()
    }

    /**
      * 通过phoenix读取Hbase表
      *
      * @param IP    IP地址如localhost
      * @param table 所要查询的表，可以是子查询
      * @param spark SparkSession
      * @param port  端口号，默认是2181
      * @return 读取到的表，返回DF
      */
    def readFromPhoenix(IP: String, table: String, spark: SparkSession, port:String = "2181"):DataFrame = {
        val df = spark.read.format("jdbc")
            .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
            .option("url", s"jdbc:phoenix:${IP}:${port}")
            .option("dbtable", table)
            .load()
        df
    }
}