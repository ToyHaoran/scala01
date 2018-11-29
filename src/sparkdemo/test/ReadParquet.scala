package sparkdemo.test

import org.apache.spark.sql.{AnalysisException, SparkSession}
import utils.BaseUtil._
import utils.{ConnectUtil, HDFSUtil}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/16
  * Time: 10:54 
  * Description:
  */
object ReadParquet extends App{

    val 读取文件大小_并且打印key的记录数 = 0
    if(0){
        val spark = ConnectUtil.spark
        val hdfsRoot = "hdfs://172.20.32.163:8020"
        val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
        //读取xxx目录下的所有parquet文件
        val files = hdfsUtil.list("/YXFK/compute/")
        //需要统计的列名
        val colName = "DQBM"

        val (res1, time1) = getMethodRunTime(hdfsUtil.readAllFileSize(spark, files, colName))
        println("一共运行" + time1)

        //sleepApp()
    }


    val 读取所有的parquet并且统一做一些处理 = 0
    if(1){
        val spark = ConnectUtil.spark
        val hdfsRoot = "hdfs://172.20.32.163:8020"
        val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
        //读取xxx目录下的所有parquet文件
        val files = hdfsUtil.list("/YXFK/compute/")
        //val files = hdfsUtil.list("/YXFK/increment/20181126_UPDATE")


        println("文件个数：" + files.length)
        //不用并行读取的原因是防止打印的时候混乱。
        for (filePath <- files) {
            try{
                //抛出异常 将不符合的路径排除掉
                val df = spark.read.parquet(hdfsRoot + filePath)
                val num = df.count()
                //对parquet进行处理
                println(filePath + ":" + num + "================================")
                //df.show(5)
                df.filter("CZSJ = '2018-11-27 19:10:55'").show()
                //df.printSchema()
            }catch {
                case ex:AnalysisException =>
                    println(s"${filePath}不是正常的parquet文件,读取失败========")
            }
        }
        //sleepApp()
    }

    val 读取某几个parquet并分别做一些处理 = 0
    if(0){
        val spark = ConnectUtil.spark
        val hdfsRoot = "hdfs://172.20.32.163:8020"
        val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
        val files = hdfsUtil.list("/YXFK/compute/")

        //需要测试的路径,注意必须是大写
        val PATH01 = "/YXFK/compute/KH_YDKH"
        val PATH02 = "/YXFK/compute/ZW_FK_YCCBSJ"

        for (filePath <- files) {
            try {
                filePath match {
                    //修改这个条件
                    case _ if filePath == PATH01 || filePath == PATH02  =>
                        val df = spark.read.parquet(hdfsRoot + filePath)
                        val num = df.count()
                        //对parquet进行处理
                        println(filePath + ":" + num + "================================")
                        df.show()
                    case _ =>

                }
            } catch {
                case ex: AnalysisException =>
                    println(s"${filePath}不是正常的parquet文件,读取失败========")
            }
        }

        //sleepApp()
    }



}