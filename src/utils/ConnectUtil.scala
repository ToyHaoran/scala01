package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ConnectUtil {
  private final val NAME = "lihaoran"

  //提示信息水平
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  //公平调度程序
  private lazy val sparkBuilder = SparkSession.builder().config("spark.scheduler.mode", "FAIR")

  /**
    * 获得SparkSession，已自动区分本机和集群
    */
  lazy val spark: SparkSession = {
    val name = scala.sys.props.get("user.name").head
    println("用户名为：" + name + "========")
    if (name.equals("llhr")) {
      println("匹配到本地spark=======")
      sparkBuilder.appName(NAME).master("local[*]").getOrCreate()
    } else {
      println("匹配到集群spark=======")
      sparkBuilder.appName(NAME).getOrCreate()
    }

  }

  /**
    * 获得SparkContext，已自动区分本机和集群
    */
  lazy val sc: SparkContext =
    if (!scala.sys.props.get("user.name").head.equals("root")) {
      new SparkContext(new SparkConf().setAppName(NAME).setMaster("local[*]"))
    } else {
      new SparkContext(new SparkConf().setAppName(NAME).setMaster("yarn-cluster"))
    }

  private val 参数详解 = 0
  /*
  设置自动广播：config("spark.sql.autoBroadcastJoinThreshold", "209715200")
      会自动广播小于10M的表，broadcast表的最大值10M（10485760），当为-1时，broadcasting不可用，内存允许的情况下加大这个值
  spark.sql.shuffle.partitions 当join或者聚合产生shuffle操作时， partitions的数量，
      这个值可以调大点， 我一般配置500， 切分更多的task， 有助于数据倾斜的减缓， 但是如果task越多， shuffle数据量也会增多

   */

}