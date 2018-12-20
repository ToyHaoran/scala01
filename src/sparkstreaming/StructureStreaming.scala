package sparkstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
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
  集群启动命令：
    /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 5g --executor-memory 5g --executor-cores 4  --num-executors 5  --class sparkdemo.StructureStreaming --name DBtest-lhr /usr/local/jar/lihaoran/lhrtest.jar
  报错：
    1、shell启动：java.net.ConnectException: Connection refused
      是因为你的端口还没有启动。
      解决方法：
        集群启动两个窗口，窗口1敲命令：nc -lk 9787
        窗口2启动spark shell，然后启动程序。
        在窗口1输入信息，然后窗口2就能接收到。
   */
  val 快速入门 = 0
  if (0) {
    val lines = spark.readStream.format("socket")
        .option("host", "localhost").option("port", 9787)
        .load()
    println(lines.isStreaming)
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream
        //.outputMode("complete") //complete：所有内容都输出  append：新增的行才输出  update：更新的行才输出
        .outputMode(OutputMode.Append())
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
  val 输入数据 = 0
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

  /*
  集群启动命令：
    nc -lk 9787
    /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 5g --executor-memory 5g --executor-cores 4  --num-executors 5  --class sparkdemo.StructureStreaming --name DBtest-lhr /usr/local/jar/lihaoran/lhrtest.jar
   */
  val 窗口操作 = 0
  if (0) {
    //窗口长度10秒，滑动距离5s，也就是说每隔5秒统计10秒内的单词数。详细见官方文档。
    val windowDuration = s"10 seconds" //窗口长度
    val slideDuration = s"5 seconds" //窗口滑动距离

    //原始schema：value, timestamp
    val lines = spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port", 9787)
        .option("includeTimestamp", value = true) //添加时间戳
        .load()
    //将word切割为多行
    val words = lines.withColumn("word", explode(split(col("value"), " ")))
    //注意window需要引入：import org.apache.spark.sql.functions._
    val human = udf((timestr: String) => timestr.substring(11, 19))
    val windowedCounts = words
        /*
          迟到10秒后的数据不再接收（默认是接收的）,详细过程
          使用Watermark的条件：http://4d3810a1.wiz03.com/share/s/1de12x0ssAHg2vQr4b1tgYE_3oiN6O2_vQQD2La-SO2_Bmvm
         */
        .withWatermark("timestamp", "10 seconds")
        .groupBy(window($"timestamp", windowDuration, slideDuration), $"word")
        .count().orderBy("window") //注意这里最好打印一下schema，否则getItem容易不匹配。
        .withColumn("start", human($"window".getItem("start")))
        .withColumn("end", human($"window".getItem("end")))
        .select("window", "start", "end", "word", "count")
    windowedCounts.printSchema()
    val query = windowedCounts.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()
    query.awaitTermination()
  }


  val join操作 = 0
  if (0) {
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9787).load()
    val wordCounts = lines.as[String].flatMap(_.split(" ")).groupBy("value").count()
    //流DF与静态DF的join
    val staticDF = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("bbb", 1, 5), ("bbb", 2, 1), ("ccc", 4, 5), ("bbb", 4, 6))).toDF("value", "key2", "key3")
    //注意这里有很多join类型不支持，详细见：http://4d3810a1.wiz03.com/share/s/1de12x0ssAHg2vQr4b1tgYE_3oiN6O2_vQQD2La-SO2_Bmvm
    val res = wordCounts.join(staticDF, Seq("value"), joinType = "inner")
    val query = res.writeStream
        .outputMode(OutputMode.Complete())
        .format("console")
        .start()
    query.awaitTermination()
    /*
    然后输入：
      nc -lk 9787   aaa   bbb   bbb
    看效果：
     */
  }

  val 查询及输出数据 = 0
  if (0) {
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9787).load()
    val wordCounts = lines.as[String].flatMap(_.split(" ")).groupBy("value").count()
    val res = wordCounts.dropDuplicates("value")
    val writeStream = res.writeStream
    //文件接收器 - 将输出存储到目录。
    writeStream.format("parquet") // can be "orc", "json", "csv", etc.
        .option("path", "path/to/destination/dir")
        .start()
    //Kafka sink - 将输出存储到Kafka中的一个或多个主题。
    writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
        .option("topic", "updates")
        .start()
    //Foreach接收器 - 对输出中的记录运行任意计算。
    /*writeStream
        .foreach()
        .start()*/
    // 控制台接收器（用于调试） - 每次触发时将输出打印到控制台/标准输出。支持Append和Complete输出模式。
    // 这应该用于低数据量的调试目的，因为在每次触发后收集整个输出并将其存储在驱动程序的内存中。
    writeStream
        .format("console")
        .start()
    // 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持Append和Complete输出模式。
    // 这应该用于低数据量的调试目的，因为整个输出被收集并存储在驱动程序的内存中。因此，请谨慎使用。
    writeStream
        .format("memory")
        .queryName("tableName")
        .start()
  }

}