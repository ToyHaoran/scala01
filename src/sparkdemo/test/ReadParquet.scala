package sparkdemo.test

import org.apache.spark.sql.{AnalysisException, SparkSession}
import utils.BaseUtil._
import utils.{ConnectUtil, HDFSUtil, PropUtil}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/16
  * Time: 10:54 
  * Description:
  */
object ReadParquet extends App {

  val 读取文件大小_并且打印key的记录数 = 0
  if (0) {
    val spark = ConnectUtil.spark
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.162")
    val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
    //不知道表名的情况下读取所有的parquet文件
    //val files = hdfsUtil.list("/YXFK/compute/")
    //知道表名的情况下直接读取
    val tables = "KH_YDKH_TEMP"
    val files = tables.split(",").map(s => "/YXFK/compute/" + s).toList.toArray
    //需要统计的列名
    val colName = "GDDWBM"
    colName.split(",").foreach(name => {
      val (res1, time1) = getMethodRunTime(hdfsUtil.readAllFileSize(spark, files, name))
      println("一共运行" + time1)
      println("=======================================================================================")
    })
    //sleepApp()
  }


  val 读取所有的parquet并且统一做一些处理 = 0
  if (0) {
    val spark = ConnectUtil.spark
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.162")
    val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
    //读取xxx目录下的所有parquet文件
    //val files = hdfsUtil.list("/YXFK/increment/20181126_UPDATE")
    val files = hdfsUtil.list("/YXFK/compute/")


    println("文件个数：" + files.length)
    //不用并行读取的原因是防止打印的时候混乱。
    for (filePath <- files) {
      try {
        //抛出异常 将不符合的路径排除掉
        val df = spark.read.parquet(hdfsRoot + filePath).select("GDDWBM")
        //对parquet进行处理
        val num = df.count()
        println(filePath + ":" + num + "================================")
        df.printSchema()
        //df.show(5)
        //df.filter("CZSJ = '2018-11-27 19:10:55'").show()
        //df.printSchema()
      } catch {
        case ex: AnalysisException =>
          //println(s"${filePath}不是正常的parquet文件,读取失败========")
          println(s"${filePath}无此字段,读取失败========")
      }
    }
    //sleepApp()
  }

  val 读取某几个parquet并分别做一些处理 = 0
  if (1) {
    val spark = ConnectUtil.spark
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.162")

    val tables = "FW_KFGDXX"
    val files = tables.split(",").map(s => "/YXFK/compute/" + s).toList.toArray

    for (filePath <- files) {
      try {
        val df = spark.read.parquet(hdfsRoot + filePath)
        val num = df.count()
        println(filePath + ":" + num + "================================")
        df.show()
        println(df.schema)
        df.printSchema()
        println(df.schema.foreach(println(_)))
        /*val df2 = df.filter("CZSJ = '2018-12-05 19:10:55'")
        df2.show()
        println(df2.count())*/
      } catch {
        case ex: AnalysisException =>
          println(s"${filePath}不是正常的parquet文件,读取失败========")
      }
    }

    //sleepApp()
  }


}