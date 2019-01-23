package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.immutable.ListMap

//导入对应的规则类，以免出现警告
import scala.language.implicitConversions

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/10/29
  * Time: 14:52 
  * Description:
  */
object BaseUtil {
  //延迟加载，用到的时候再加载
  lazy val spark = ConnectUtil.spark

  import spark.implicits._

  /**
    * 用来快捷控制代码的开关，将0，1隐式转换为false和true
    */
  implicit def int2boolen(a: Int): Boolean = {
    if (a == 0) false else true
  }

  /**
    * 得到参数的类型
    */
  def getTypeName(a: Any): String = {
    a.getClass.getSimpleName
  }

  /**
    * 得到代码块的运行时间
    *
    * @param block 需要测试的代码块
    * @return Turple(代码块返回值 | 代码运行时间)
    */
  def getMethodRunTime[R](block: => R): (R, String) = {
    val start = System.nanoTime() //系统纳米时间
    val result = block
    val end = System.nanoTime()
    val delta = end - start
    val ms = delta / 1000000d //毫秒
    val s = ms / 1000d //秒
    val min = s / 60d //分钟
    (result, s.formatted("%.3f") + "s") //保留三位小数  单位 秒
    //(result, min.formatted("%.2f") + "min") //保留两位小数   单位 分钟
  }

  /**
    * 用于睡眠程序（默认10分钟），以查看spark UI
    */
  def sleepApp(ms: Long = 1000 * 60 * 10): Unit = {
    println("正在睡眠，持续10分钟..............")
    Thread.sleep(ms)
  }

  val DataFrame相关工具方法 = 0

  /**
    * DF的装饰类（隐式转换）
    */
  class RichDataFrame(df: DataFrame) {
    /**
      * 用来统计相同key的记录数，常用于调整数据倾斜
      */
    def printKeyNums(column: Column): Unit = {
      var map = df.select(column).rdd.countByValue()
      println(s"一共${map.size}个key")
      //对map进行排序（默认从小到大）
      map = ListMap(map.toSeq.sortBy(_._2): _*)
      for ((key, num) <- map) {
        println(key + "共有" + num + "条记录")
      }
    }

    def printKeyNums(column: String): Unit = {
      printKeyNums(df.col(column))
    }

    /**
      * 每个元素在分区位置信息
      */
    def printItemLoc(): Unit = {
      println("每个元素在分区位置信息如下，不能打印太多元素==============")
      df.rdd.printItemLoc()
    }

    /**
      * 每个元素在分区的位置信息，只选择特征列
      */
    def printItemLoc(column: String): Unit = {
      df.select(column).rdd.printItemLoc()
    }

    /**
      * 打印每个分区的元素数量
      */
    def printPartItemNum(): Unit = {
      df.select(df.schema.head.name).rdd.printPartItemNum()
    }

    /**
      * 得到每个分区对应的元素个数DF
      */
    def getPartItemNum(): DataFrame = {
      df.select(df.schema.head.name).rdd
          .mapPartitionsWithIndex { case (partIdx, iter) =>
            Iterator(("part_" + partIdx, iter.size))
          }.toDF("partition", "num")
    }
  }

  /**
    * 扩展df的方法，隐式转换
    */
  implicit def df2RichDF(src: DataFrame): RichDataFrame = new RichDataFrame(src)

  /**
    * 打印map信息
    * 使用了泛型
    */
  def printMap(map: Map[_ <: Any, _ <: Any]): Unit = {
    for ((key, value) <- map) {
      try {
        println(key.toString + " : " + value.toString)
      } catch {
        case e: NullPointerException =>
          println(s"${key} : 为空")
      }
    }
    println("===================")
  }


  val RDD相关工具方法 = 0

  /**
    * RDD的装饰类（隐式转换）,不加泛型读取不到
    *
    */
  class RichRDD(rdd: RDD[_ <: Any]) {

    /**
      * 每个元素在分区位置信息，不能打印太多元素
      */
    def printItemLoc(): Unit = {
      println("分区位置信息========================")
      rdd.mapPartitionsWithIndex { case (index, iter) =>
        Iterator(s"part_$index：${iter.mkString(",")}")
      }.collect().foreach(println(_))
    }

    /**
      * 打印每个分区的元素数量
      */
    def printPartItemNum(): Unit = {
      println("每个分区的元素数量如下=============")
      rdd.mapPartitionsWithIndex { case (partIdx, iter) =>
        Iterator(("part_" + partIdx, iter.size))
      }.collect().foreach(println)
    }
  }


  /**
    * 扩展RDD的方法，隐式转换
    */
  implicit def rdd2RichRDD(src: RDD[_ <: Any]): RichRDD = new RichRDD(src)


}