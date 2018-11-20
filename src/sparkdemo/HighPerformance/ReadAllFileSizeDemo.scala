package sparkdemo.HighPerformance

import utils.{ConnectUtil, HDFSUtil}
import utils.BaseUtil._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/16
  * Time: 10:54 
  * Description:
  */
object ReadAllFileSizeDemo {
    def main(args: Array[String]) {
        val spark = ConnectUtil.getClusterSpark
        import spark.implicits._
        val hdfsRoot = "hdfs://172.20.32.164:8020"
        val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
        //读取xxx目录下的所有parquet文件
        val files = hdfsUtil.list("/YXFK/compute/")
        //需要统计的列名
        val colName = "GDDWBM"

        val (res1, time1) = getMethodRunTime(hdfsUtil.readAllFileSize(spark, files, colName))
        println("一共运行" + time1)

        Thread.sleep(1000 * 60 * 10)
    }
}