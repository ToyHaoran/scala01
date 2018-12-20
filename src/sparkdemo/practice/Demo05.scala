package sparkdemo.practice

//某市公安局要对全市的安检进行数字化布防，假设你是其中的一名干警，根据领导要求要对全市的旅馆进行数据分析，现在你有以下数据：
//表A:重点监控人员的身份证号名单。(ID String)
//表B:旅馆的入住人员身份证号和入店出店时间。(ID String, INTIME String(yyyy-MM-dd HH:mm:ss), OUTTIME String(yyyy-MM-dd HH:mm:ss))
//注：以上数据均为模拟数据！
//试分析：
//1、全部旅客的两次之间的住店间隔，即每个旅客每隔多长时间住店一次（以天为单位，第一次无间隔，记为0）。
//2、全部旅客的每次住店时常，即每个旅客每次住店多长时间（小时）。
//3、全部旅客的每次入店时间和出店时间（以小时为单位，四舍五入），
//    比如李华2018-08-15 12:01:00入店，则入店时间记为12，2018-08-15 21:49:00出店，则记为22。
//4、正常旅客和重点人员的的住店间隔、住店时常、入店时间和出店时间的统计：
//    众数，中位数，平均值，最大值，最小值，每个时间段的计数（如：住店间隔为2天的为30次，住店时常为8小时的为60次）。

//数据示例
//tableA
//{"ID": "daae2049-31ea-42f3-82d4-167db8a32f52"}
//{"ID": "ef73789f-c790-47a7-beac-de161cd908ff"}
//tableB
//{"ID": "9c05ff3b-7582-46d5-acd5-5b788ca2209e", "INTIME": "2016-03-16 19:34:13", "OUTTIME": "2016-03-17 00:34:13"}
//{"ID": "c975ba39-9cf1-4c81-915c-3790ccad5a84", "INTIME": "0115-01-23 21:49:04", "OUTTIME": "2015-01-24 03:49:04"}


import java.sql.Date
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import utils.ConnectUtil

import scala.collection.mutable.ArrayBuffer

object Demo05 extends App {
  val sc = ConnectUtil.sc
  val spark = ConnectUtil.spark

  import spark.implicits._

  //读取文件，返回DF
  val dfa = spark.read.json("src/data/sql/exam3_table_A.json")
  val dfb = spark.read.json("src/data/sql/exam3_table_B.json")


  //可以不用创建临时表，直接用df做
  dfa.createOrReplaceTempView("dfa")
  dfb.createOrReplaceTempView("dfb")


  //——————————————————————————————————————————————————————————————
  //1、全部旅客的两次之间的住店间隔，即每个旅客每隔多长时间住店一次（以天为单位，第一次无间隔，记为0）。
  //主要问题是如何令每个顾客的入住时间相减？用for循环
  //思路1：（后来发现太麻烦了）用UDAF，注意那些异常的数据是正常的，居然有0111年入住的。。。。
  spark.udf.register("timeinterval", new Interval)
  spark.sql("select ID,timeinterval(INTIME) from dfb group by ID")

  def getxxxx: UserDefinedAggregateFunction = {
    new Interval
  }

  val winSpec = Window.partitionBy("seller_id").orderBy("pay_time").rowsBetween(-1, 0)
  dfb.withColumn("indddd", getxxxx($"ID").over(winSpec))

  //UDAF类定义
  class Interval extends UserDefinedAggregateFunction {
    //继承抽象函数必须实现以下方法

    //输入参数的数据类型
    def inputSchema: StructType = StructType(StructField("input", StringType) :: Nil)

    // 缓冲区中进行聚合时，所处理的数据的类型
    def bufferSchema: StructType = {
      StructType(StructField("sum", StringType) :: StructField("count", LongType) :: Nil)
    }

    // 返回值的数据类型，只能返回一列
    //    def dataType: DataType = StringType
    def dataType: StructType = {
      StructType(StructField("interval", StringType) :: StructField("count", LongType) :: Nil)
    }


    // 初始化给定的聚合缓冲区，即聚合缓冲区的零值。
    // 请注意，缓冲区内的数组和映射仍然是不可变的。
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = "" //用来存储所有的日期毫秒值
      buffer(1) = 0L //用来存储住店的次数
    }

    //使用来自`input`的新输入数据更新给定的聚合缓冲区`buffer`。每个输入行调用一次。
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = format.parse(input.getString(0))
        val ms = date.getTime //得到毫秒值
        buffer(0) = buffer.getString(0) + ms.toString + " "
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    // 此函数是否始终在相同输入上返回相同的输出
    def deterministic: Boolean = true

    // Spark是分布式的，所以不同的区需要进行合并。
    // 合并两个聚合缓冲区并将更新的缓冲区值存储回“buffer1”。当我们将两个部分聚合的数据合并在一起时调用此方法。
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getString(0) + buffer2.getString(0) + " "
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //计算最终的结果
    def evaluate(buffer: Row): (String, Long) = {
      val arr = buffer.getString(0).split("\\s+").map(_.toDouble).sorted
      val length = arr.length
      var res = "0 "
      for (i <- 0 to length - 2) {
        val j = i + 1
        val res1 = ((arr(j) - arr(i)) / 86400000).round
        res = res + res1.toString + " "
      }
      (res, buffer.getLong(1))
    }
  }


  //思路2：用DF，推荐，
  //有问题，看看有没有得到负数的毫秒值（比如1970年之前入住的。）然后没有处理导致出错的。。。
  //检查后没问题。其实也可以把异常数据直接过滤掉（但是那就没有处理全部数据了）
  dfb.groupByKey(_.getAs[String]("ID")).mapGroups((id, groups) => {
    //用来收集每个人的入住时间
    var arr = new ArrayBuffer[Double]()
    //入住次数
    var num = 0
    //定义日期转换,将字符串转为日期类型
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (i <- groups) {
      val ms = format.parse(i.getString(1)).getTime //得到的负数只能用来求差值。
      arr += ms.toDouble
      num = num + 1
    }
    arr = arr.sorted //排序后，负数在前面，正常
    val length = arr.length
    var res = "0 "
    for (i <- 0 to length - 2) {
      val j = i + 1
      val res1 = ((arr(j) - arr(i)) / 86400000).round //两个日期的毫秒值差值，负数情况下相减得到正数，正常
      res = res + res1.toString + " "
    }
    (id, res, num)
  }).toDF("ID", "intervalStr", "num").show()





  //——————————————————————————————————————————————————————————————
  //2、全部旅客的每次住店时长，即每个旅客每次住店多长时间（小时）。
  spark.udf.register("minusHour", (in: String, out: String) => {
    val loc = new Locale("en") //时间格式改为本地
    val fm = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"), loc)
    val intime = fm.parse(in)
    val outtime = fm.parse(out)
    val minusHour = (outtime.getTime - intime.getTime) / 3600000 //获取相差的小时数，负数情况下正常
    minusHour
  })
  //推荐
  dfb.select(expr("ID"), expr("minusHour(INTIME,OUTTIME) as minusHour")).show()
  //或
  spark.sql("select ID,INTIME,OUTTIME,minusHour(INTIME,OUTTIME) as minusHour from dfb")


  //——————————————————————————————————————————————————————————————————
  //3、全部旅客的每次入店时间和出店时间（以小时为单位，四舍五入），
  //    比如李华2018-08-15 12:01:00入店，则入店时间记为12，2018-08-15 21:49:00出店，则记为22。
  // 思路1：注意Long/3600000不会保留小数。因此要转换为Double,然后/3600000，然后.round,最后减去天数*24，就是结果
  // （失败，问题1：毫秒值有负数（1970年之前）不好处理。问题2：得到的小时和分钟是距离1970年的时间，并不是实际的小时，还需要转换）
  //思路2：直接用过期的方法得到小时和分钟。
  spark.udf.register("hour", (in: String) => {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val intime = fm.parse(in)
    val ms = intime.getTime
    val hour = intime.getHours
    val min = ((intime.getMinutes).toDouble / 60).round
    val res = hour + min
    if (res >= 24) {
      res - 24
    } else {
      res
    }
  })
  //思路3：用Calender方法。推荐
  spark.udf.register("hour", (in: String) => {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val intime = fm.parse(in)
    val calender = Calendar.getInstance()
    calender.setTime(intime)
    val hour = calender.get(Calendar.HOUR_OF_DAY) //24小时制
    val min = (calender.get((Calendar.MINUTE)).toDouble / 60).round

    val res = hour + min
    if (res >= 24) {
      res - 24
    } else {
      res
    }
  })

  dfb.withColumn("intime", expr("hour(INTIME)")).withColumn("outtime", expr("hour(OUTTIME)")).show()
  //推荐
  dfb.selectExpr("ID", "hour(INTIME) as in", "hour(OUTTIME) as out")
  spark.sql("select ID,INTIME,hour(INTIME) as in,OUTTIME,hour(OUTTIME) as out from dfb").show


  //————————————————————————————————————————————————————————————————
  //4、正常旅客和重点人员的的住店间隔、住店时常、入店时间和出店时间的统计：
  //    众数，中位数，平均值，最大值，最小值，每个时间段的计数（如：住店间隔为2天的为30次，住店时常为8小时的为60次）。

  //问题：如果求时间间隔必须分组，而分组后又没法用普通的聚合函数，因此只能先聚合完之后，然后再进行拆分计算次数。
  //感觉可以在前面建立临时表，然后这里进行连接统计。

  /* 实现以下效果
  scala> dfc.show(100,20)
+--------------------+--------------------+--------------------+--------------------+--------------------+---+
|                  ID|            interval|               hours|              intime|             outtime|num|
+--------------------+--------------------+--------------------+--------------------+--------------------+---+
|013f495a-5c43-4b3...|0 1 5 2 6 7 4 4 8...|9 12 9 9 10 6 8 1...|14 0 17 23 14 19 ...|23 12 2 8 0 1 7 8...| 16|
|0a07fcb5-3bf9-4cd...|0 1 4 2 1 4 8 3 0...|8 12 11 10 12 11 ...|16 13 15 14 21 13...|0 1 2 0 9 0 20 3 ...| 11|
|136837da-310f-424...|  0 6 4 7 5 0 5 4 6 |12 8 8 8 7 12 7 1...|13 18 0 21 17 18 ...| 1 2 8 5 0 6 4 6 10 |  9|
|14ff797b-8c9c-47a...|            0 2 5 7 |           11 7 9 8 |        19 23 22 20 |            6 6 7 4 |  4|
|1855c76a-3419-4e3...|    0 1 0 2 4 7 5 4 | 10 10 11 9 9 9 5 8 |18 14 22 13 17 19...|  4 0 9 22 2 4 18 7 |  8|
|1ab2a414-6236-41b...|      0 5 3 2 3 2 7 |   6 6 10 10 12 9 7 |16 18 13 17 13 19...|    22 0 23 3 1 4 1 |  7|
   */

  case class Statistics(ID: String, interval: String, hours: String, intime: String, outtime: String, num: Int)

  val dfc = dfb.groupByKey(_.getAs[String]("ID")).mapGroups((id, groups) => {
    //用来保存住店时间的毫秒值
    var arr = new scala.collection.mutable.ArrayBuffer[Double]()
    //住店次数
    var num = 0
    //住店时常
    var hourstr = ""
    //入店时间
    var intimestr = ""
    //出店时间
    var outtimestr = ""

    //传入日期如19:46，四舍五入转为20
    //例题3的方法
    val getHour = (in: java.util.Date) => {
      val calender = Calendar.getInstance()
      calender.setTime(in)
      val hour = calender.get(Calendar.HOUR_OF_DAY) //24小时制
      val min = (calender.get((Calendar.MINUTE)).toDouble / 60).round
      val res = hour + min
      if (res >= 24) {
        res - 24
      } else {
        res
      }
    }
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //定义日期转换
    for (i <- groups) {
      val intime = format.parse(i.getString(1))
      val outtime = format.parse(i.getString(2))
      val inms = intime.getTime
      val outms = outtime.getTime
      //入住时间保存到可变数组中，后面用来排序求时间间隔
      arr += inms.toDouble
      //住店次数
      num = num + 1
      //住店时常,单位H(注意毫秒值只能用来求时间差，不能干其他的，因为有负数)
      hourstr += ((outms - inms) / 3600000).toString + " "
      //入店时间，单位：H
      intimestr += getHour(intime).toString + " "
      //出店时间，单位：H
      outtimestr += getHour(outtime).toString + " "
    }
    //对入住时间毫秒值进行排序
    arr = arr.sorted
    val length = arr.length
    var res = "0 "
    //两两相减，保存到字符串中
    for (i <- 0 to length - 2) {
      val j = i + 1
      val res1 = ((arr(j) - arr(i)) / 86400000).round
      res = res + res1.toString + " "
    }
    Statistics(id, res, hourstr, intimestr, outtimestr, num) //也可以直接toDF("","")
  }).toDF()

  //众数，中位数，平均值，最大值，最小值，每个时间段的计数（如：住店间隔为2天的为30次，住店时常为8小时的为60次）
  dfc.createOrReplaceTempView("dfc")

  //重点人员(暂时没想到怎么用DF筛选重点人员，只能用SQL了)
  val dfd = spark.sql("select c.* from dfc c where c.ID in(select ID from dfa)")
  //所有的间隔时间数据
  val rddfalt = dfd.select("interval").rdd.flatMap(_.getString(0).split(" ")).filter(_ != "0")
  rddfalt.cache()
  //每个时间段的计数统计，
  // 对于单列来说，df和rdd的操作方式是一样的。但是DS没有reduceByKey函数，需要用聚合函数
  // 所以这里推荐使用RDD
  rddfalt.map(x => (x.toLong, 1)).reduceByKey((v1, v2) => {
    v1 + v2
  }).sortByKey().toDF("interval", "num").createOrReplaceTempView("dfg")
  //sql中必须加别名，否则报错
  //求众数，为60，次数为444
  spark.sql("select g.interval as MaxOfManyInterval,g.num from dfg g where g.num = (select max(num) from dfg)")
  //求中位数，Array((51,500))
  var seq = 0L
  val count = dfd.count()
  val rddh = rddfalt.map(_.toLong).sortBy(c => c, true).zipWithIndex().filter(_._2 == count / 2)
  //求平均值,最大值，最小值
  //  |       avg(value)|max(value)|min(value)|
  //    +-----------------+----------+----------+
  //  |70460.58576938693|    694006|        41|
  val dfvalue = rddfalt.map(_.toLong).toDF("value").createOrReplaceTempView("dfvalue")
  spark.sql("select avg(a.value),max(a.value),min(a.value) from dfvalue a ")
  //同理，其他的数据也一样求。

  //其他的可以同上（注意把过滤条件去掉），也可以在未分组的情况下进行统计

  //普通旅客
  val dff = spark.sql("select c.* from dfc c where c.ID not in(select ID from dfa)")


}

