package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ConnectUtil {
    private final val COMPANY = "llhr"
    private final val WHITE = "lihaoran"

    //提示信息水平
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO) //Level.WARN

    //公平调度程序
    private lazy val sparkBuilder = SparkSession.builder().config("spark.scheduler.mode", "FAIR")

    /**
      * 获得SparkSession，已自动区分本机和集群
      */
    lazy val spark: SparkSession =
        if (scala.sys.props.get("user.name").head.equals(COMPANY)) {
            println("匹配到公司笔记本spark=======")
            sparkBuilder.appName("LIHAORAN").master("local[*]").getOrCreate()
        } else if (scala.sys.props.get("user.name").head.equals(WHITE)) {
            println("匹配到宿舍笔记本spark==========")
            sparkBuilder.appName("LIHAORAN").master("local[*]").getOrCreate()
        } else {
            println("匹配到公司集群spark=========")
            sparkBuilder.appName("LIHAORAN").getOrCreate()
        }


    /**
      * 获得SparkContext，已自动区分本机和集群
      */
    lazy val sc:SparkContext =
        if(scala.sys.props.get("user.name").head.equals(COMPANY)){
            new SparkContext(new SparkConf().setAppName("SparkContextDemo").setMaster("local[*]"))
        }else if(scala.sys.props.get("user.name").head.equals(WHITE)){
            new SparkContext(new SparkConf().setAppName("SparkContextDemo").setMaster("local[*]"))
        }else{
            new SparkContext(new SparkConf().setAppName("SparkContextDemo").setMaster("yarn-cluster"))
        }

}