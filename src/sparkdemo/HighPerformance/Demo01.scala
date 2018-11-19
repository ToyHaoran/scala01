package sparkdemo.HighPerformance

import utils.{ConnectUtil, HDFSUtil}
import utils.BaseUtil._
import utils.DateUtil._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/16
  * Time: 10:54 
  * Description:
  */
object Demo01 {
    def main(args: Array[String]) {
        val spark = ConnectUtil.getClusterSpark
        import spark.implicits._
        val hdfsRoot = "hdfs://172.20.32.163:8020"
        val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
        val files = hdfsUtil.list("/YXFK/compute/")

        val (res1, time1) = getMethodRunTime(hdfsUtil.readAllFileSize(spark,files))
        println("一共运行"+time1)

        //val df = spark.read.parquet("")



        Thread.sleep(1000 * 60 * 10)
    }

}