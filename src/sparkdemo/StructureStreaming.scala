package sparkdemo

import org.apache.spark.sql.types.StructType
import utils.BaseUtil._
import utils.ConnectUtil

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/12/11
  * Time: 17:17 
  * Description:
  */
object StructureStreaming extends App {
  val spark = ConnectUtil.spark

  import spark.implicits._

  /*
  参考：http://spark.apache.org/docs/2.3.1/structured-streaming-programming-guide.html
  报错：
    1、java.net.ConnectException: Connection refused
      是因为你的端口还没有启动。
      解决方法：
        集群启动两个窗口，窗口1敲命令：nc -lk 9787
        窗口2启动spark shell，然后启动程序。
        在窗口1输入信息，然后窗口2就能接收到。
   */
  val 快速入门 = 0
  if (1) {
    val lines = spark.readStream.format("socket")
        .option("host", "localhost").option("port", 9787)
        .load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream
        .outputMode("complete") //complete：所有内容都输出  append：新增的行才输出  update：更新的行才输出
        .format("console")
        .start()
    query.awaitTermination()
  }

  /*
  输入源：参考：https://blog.csdn.net/gg464556/article/details/77836206
    文件（text，csv，json，orc，parquet）：可以直接点出来方法。
    Kafka Source：
    Socket source (for testing)：必须指定要连接的主机和端口，无容错。
    Rate source (for testing)：
   */
  val 读取数据 = 0
  if (0) {
    //封装的
    spark.readStream.csv("")
    spark.readStream.text("")
    spark.readStream.textFile("")
    spark.readStream.parquet("")
    spark.readStream.json("")
    //通用加载
    spark.readStream.format("socket").option("host", "localhost").option("port", 9787).load()
    // 读取目录内原子写入的所有 csv 文件
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark.readStream.option("sep", ";").schema(userSchema) // 指定 csv 文件的模式
        .csv("/path/to/directory") // 等同于 format("csv").load("/path/to/directory")
  }

}