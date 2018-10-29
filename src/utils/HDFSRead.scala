package utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/10/29
  * Time: 9:15 
  * Description:
  */
object HDFSRead {
    def main(args: Array[String]) {
        val spark: SparkSession = SparkSession.builder().appName("测试文件大小和数据").getOrCreate()
        val hdfsRoot = "hdfs://172.20.32.164:8020"
        val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
        val files = hdfsUtil.list("/YXFK/compute/")
        println("所有文件大小：" + files.length)
        println("大于100万的表记录数========")
        val list1:List[String] = List()
        for (filePath <- files) {
            if(filePath != "/YXFK/compute/HS_DJWDMX" && filePath !="/YXFK/compute/open"){
                val df = spark.read.parquet(hdfsRoot + filePath)
                val num = df.count()
                if(num > 1000000){
                    println(filePath + ":" + df.count())
                    list1.:+(filePath)
                    // 对大于100万的数据进行处理
                    // 抽取十分之一的数据进行统计
                    val rdd = df.select("DQBM","GDDWBM").rdd
                    val map = rdd.sample(false, 0.1).countByValue()
                    for((dqbm,num) <- map){
                        println(dqbm + "共有" + num + "条记录" )
                    }
                    // 根据大小重分区
                    df.repartition(4, df.col("GDDWBM"))

                }
            }
        }
    }
}