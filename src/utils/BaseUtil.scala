package utils

import org.apache.spark.sql._

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
    /**
      * 用来快捷控制代码的开关，将0，1隐式转换为false和true
      */
    implicit def int2boolen(a: Int): Boolean = {
        if (a == 0) false else true
    }

    /**
      * 得到参数的类型
      */
    def getTypeName(a:Any):String = {
        a.getClass.getSimpleName
    }

    /**
      * 得到代码块的运行时间
      *
      * @param block 需要测试的代码块
      * @tparam R
      * @return Turple(代码块返回值 | 代码运行时间 毫秒值)
      */
    def getMethodRunTime[R](block: => R): (R, String) = {
        val start = System.nanoTime() //系统纳米时间
        val result = block
        val end = System.nanoTime()
        val delta = end - start
        (result, (delta / 1000000d).toString + "ms")
    }

    /**
      * 用于睡眠程序（10分钟），以查看spark UI
      */
    def sleepApp(): Unit ={
        println("程序已结束，睡眠10分钟，请查看Spark UI, 或者kill应用")
        Thread.sleep(1000 * 60 * 10)
    }

    val DataFrame相关工具方法 = 0
    /**
      * DF的装饰类（隐式转换）
      */
    class RichDataFrame(dataFrame: DataFrame){

        /**
          * 用来统计相同key的记录数，常用于调整数据倾斜
          */
        def printKeyNums(column: Column): Unit ={
            val map = dataFrame.select(column).rdd.countByValue()
            println(s"一共${map.size}个key")
            for ((key, num) <- map) {
                println(key + "共有" + num + "条记录")
            }
        }

        def printKeyNums(column: String): Unit ={
            printKeyNums(dataFrame.col(column))
        }

        /**
          * 打印分区位置信息
          */
        def printLocation(): Unit ={
            println("分区位置信息如下==============")
            dataFrame.rdd.mapPartitionsWithIndex(printLocationFunc).collect().foreach(println(_))
        }
    }

     /**
      * 扩展df的方法，隐式转换
      *
      * @return
      */
    implicit def df2RichDF(src: DataFrame): RichDataFrame = new RichDataFrame(src)

    /**
      * 打印map信息
      * 使用了泛型
      */
    def printMap(map: Map[_ <: Any, _ <: Any]): Unit = {
        for ((key, value) <- map) {
            try{
                println(key.toString + " : " + value.toString)
            }catch {
                case e:NullPointerException =>
                    println(s"${key} : 为空")
            }
        }
        println("===================")
    }


    val RDD相关工具方法 = 0

    /**
      * 打印rdd的分区信息，需要用mapPartitionsWithIndex方法。
      * 使用方法：df.rdd.mapPartitionsWithIndex(printLocationFunc).collect().foreach(println(_))
      */
    def printLocationFunc(index: Int, iter: Iterator[Any]): Iterator[String] = {
        iter.map(x => "分区" + index + "：" + x + "")
    }
}