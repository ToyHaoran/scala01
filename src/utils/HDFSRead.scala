package utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

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
        /*
            如何将上面那种方式写活？
            首先前5位编码和记录数是能够获取的。
            对于大于20万的记录数：
                然后将记录数/20，获得大致分区数。
                然后设置随机数--分区数
                然后怎么设置字段？能不能将一个列表中的元素随机返回一个？
                    循环生成一个列表，可以用80890_1
                    然后再随机生成一个随机数，然后返回list[i]
                    这样就能处理了。
            对于小于20万的记录数：
                加起来/20，为800xx
         */

        for (filePath <- files) {
            if (filePath != "/YXFK/compute/HS_DJWDMX" && filePath != "/YXFK/compute/open") {
                val df = spark.read.parquet(hdfsRoot + filePath)
                val num = df.count()
                if (num > 1000000) {
                    println(filePath + ":" + df.count())
                    println("供电单位编码(前五位)==========")
                    try {
                        val rdd2 = df.select("GDDWBM")
                            .withColumn("SUBGDDWBM", getSubgddwbm(col("GDDWBM")))
                            .drop("GDDWBM")
                            .rdd

                        val map2 = rdd2.countByValue()
                        println("共有" + map2.size + "个供电单位编码前5位")
                        for ((gddwbm, num) <- map2) {
                            println(gddwbm + "共有" + num + "条记录")
                        }
                        val df2 = df.withColumn("SUBGDDWBM", getSubgddwbm(col("GDDWBM")))
                            .withColumn("PARTITION", getPartitionFild(col("GDDWBM"), col("SUBGDDWBM"), lit(num)))
                        df2.repartition(col("PARTITION")).write.partitionBy("PARTITION").mode(SaveMode.Overwrite)
                            .parquet(hdfsRoot + "/temp_data/temp_lihaoran/")

                        println("写入完成=========")
                    } catch {
                        case ex: AnalysisException => {
                            println("没有GDDWBM字段===")
                        }
                    }
                }
            }
        }
        def getPartitionFild: UserDefinedFunction = {
            udf{(gddwbm: String, subgddwbm: String, num: Long)=>
                var res = ""
                if(num > 100000){
                    val partititonNum = (num / 200000 + 1).toInt
                    val gddwbmlist = List[String]()
                    for(i <- 1 to partititonNum){
                        gddwbmlist.+:(subgddwbm + "_" + i)
                    }
                    val ran = scala.util.Random.nextInt(partititonNum)
                    res = gddwbmlist(ran)
                }else{
                    //小于10万的基本上没有
                    res = "800xx"
                }
                res
            }
        }






        /*for (filePath <- files) {
            //读取/YXFK/compute/KH_JLD表并且重新分区
            if(filePath == "/YXFK/compute/KH_JLD"){
                val df = spark.read.parquet(hdfsRoot + filePath)
                val df2 = df.withColumn("PARTITION", getJldPartition(col("GDDWBM")))
                //这种模式是先根据col将相同列的不同分区的数据拉取到一个分区
                //df2.repartition(30, col("PARTITION")).write.mode(SaveMode.Overwrite).parquet(hdfsRoot + "/temp_data/temp_lihaoran/")
                //
                //df2.repartition(col("PARTITION")).write.mode(SaveMode.Overwrite).parquet(hdfsRoot + "/temp_data/temp_lihaoran/")

                //这种模式是按照列名分区，但是每个列文件中有不少分区，是因为没有将不同分区的数据拉去到同一分区。
                //df2.write.partitionBy("PARTITION").mode(SaveMode.Overwrite).parquet(hdfsRoot + "/temp_data/temp_lihaoran/")
                df2.repartition(29, col("PARTITION")).write.partitionBy("PARTITION").mode(SaveMode.Overwrite).parquet(hdfsRoot + "/temp_data/temp_lihaoran/")

                // 查看保存的信息
                /*val df3 = spark.read.parquet(hdfsRoot + "/temp_data/temp_lihaoran/")
                df3.show*/
            }
        }*/

    }


    def getJldPartition: UserDefinedFunction = {
        udf{(gddwbm: String) => {
            val sub5 = if(!gddwbm.endsWith("00")){
                gddwbm.substring(0, 5)
            }else{
                gddwbm
            }
            val partition:String = sub5 match {
                case "80014" =>{
                    if(gddwbm == "80014" || gddwbm == "8001402" || gddwbm == "8001410" || gddwbm == "8001404"
                        || gddwbm == "8001405"){
                        //20万左右
                        "80014A"
                    }else if(gddwbm == "8001406" || gddwbm == "8001409" || gddwbm == "8001403" ){
                        //10万
                        "80014B"
                    }else{
                        //这里可不可以用一个随机数？（0-2） 可以（已证实）
                        val ran = scala.util.Random.nextInt(3)
                        if(ran == 0){ // 10万
                            "80014B"
                        }else{ // 20万
                            "80014C"
                        }
                    }
                }
                case "80011" =>{
                    if(gddwbm == "8001102"){
                        "8001102" //20
                    }else if(gddwbm == "8001111" || gddwbm == "80011" || gddwbm == "8001114"){
                        "80011A" //22
                    }else if(gddwbm == "8001120"){
                        "80011B" //24
                    }else{
                        "80011C" //22
                    }
                }
                case "80006" =>{
                    //共60万
                    val ran = scala.util.Random.nextInt(3)
                    if(ran == 0){
                        "80006A"
                    }else if(ran == 1){
                        "80006B"
                    }else{
                        "80006C"
                    }
                }
                case "80083" =>{ //分成3分
                    if(gddwbm == "8008341"  || gddwbm == "8008308" || gddwbm == "8008307"){
                        "80083A" //20
                    }else if(gddwbm == "8008309" || gddwbm == "8008317" || gddwbm == "8008306"){
                        "80083B" //19
                    }else{
                        "80083C" //28
                    }
                }
                case "80013" =>{ //分成4分
                    if(gddwbm == "8001313"  || gddwbm == "8001305" || gddwbm == "8001301"){
                        "80013A"
                    }else if(gddwbm == "8001314" || gddwbm == "8001306" || gddwbm == "8001310" || gddwbm == "8001307"){
                        "80013B"
                    }else if(gddwbm == "8001302" || gddwbm == "8001304"){
                        "80013C"
                    }else{
                        "80013D"
                    }
                }
                case "80084" =>{ //31万
                val ran = scala.util.Random.nextInt(2)
                    if(ran == 0){
                        "80084A"
                    }else{
                        "80084B"
                    }
                }
                case "80004" =>{ //49万
                val ran = scala.util.Random.nextInt(2)
                    if(ran == 0){
                        "80004A"
                    }else{
                        "80004B"
                    }
                }
                case "80003" =>{ //46万
                val ran = scala.util.Random.nextInt(2)
                    if(ran == 0){
                        "80003A"
                    }else{
                        "80003B"
                    }
                }
                case "80005" =>{ //72万
                val ran = scala.util.Random.nextInt(3)
                    if(ran == 0){
                        "80005A"
                    }else if(ran == 1){
                        "80005B"
                    }else{
                        "80005C"
                    }
                }
                case "80015" =>{ //29万
                    "80015"
                }
                case "80012" =>{ //30万
                    "80012"
                }
                case _ =>{ //2万
                    "800xx"
                }
            }
            partition
        }
        }
    }
    /**
      * 读取所有文件大小，以及供电单位编码的前5位。
      * @param spark
      * @param hdfsRoot
      * @param files
      */
    def readAllFileSize(spark: SparkSession, hdfsRoot: String, files: Array[String]): Unit = {
        println("所有文件大小：" + files.length)
        println("大于100万的表记录数========")
        val list1: List[String] = List()
        for (filePath <- files) {
            if (filePath != "/YXFK/compute/HS_DJWDMX" && filePath != "/YXFK/compute/open") {
                val df = spark.read.parquet(hdfsRoot + filePath)
                val num = df.count()
                if (num > 1000000) {
                    println(filePath + ":" + df.count())
                    list1.:+(filePath)
                    // 对大于100万的数据进行处理
                    println("地区编码==============")
                    val rdd = df.select("DQBM").rdd
                    val map = rdd.countByValue()
                    for ((dqbm, num) <- map) {
                        println(dqbm + "共有" + num + "条记录")
                    }

                    println("供电单位编码(前五位)==========")
                    try {
                        /*//供电单位编码
                        val rdd2 = df.select("GDDWBM").rdd
                        val map2 = rdd2.countByValue()
                        println("共有" + map2.size + "个供电单位编码")
                        for ((gddwbm, num) <- map2) {
                            println(gddwbm + "共有" + num + "条记录")
                        }*/

                        // 供电单位编码(前五位)

                        val rdd2 = df.select("GDDWBM")
                            .withColumn("SUBGDDWBM", getSubgddwbm(col("GDDWBM")))
                            .drop("GDDWBM")
                            .rdd
                        val map2 = rdd2.countByValue()
                        println("共有" + map2.size + "个供电单位编码前5位")
                        for ((gddwbm, num) <- map2) {
                            println(gddwbm + "共有" + num + "条记录")
                        }

                        //自定义分区
                        df.repartition(getSubgddwbm(df.col("GDDWBM"))).write.parquet("xxxxx")

                    } catch {
                        case ex: AnalysisException => {
                            println("没有GDDWBM字段===")
                        }
                    }
                }
            }
        }
    }

    /**
      * 得到前五位的供电单位编码
      * @return
      */
    def getSubgddwbm: UserDefinedFunction = {
        udf{(gddwbm: String)=>
            if(gddwbm == null || gddwbm.trim().length == 0 || gddwbm.toLowerCase() == "null" || gddwbm.endsWith("00")){
                "80000"
            }else{
                var sub5 = ""
                if(gddwbm.startsWith("0")){
                    // 注意还有以0开头的。。。
                    sub5 = gddwbm.substring(1, 6)
                }else{
                    sub5 = gddwbm.substring(0, 5)
                }
                sub5
            }
        }
    }

}