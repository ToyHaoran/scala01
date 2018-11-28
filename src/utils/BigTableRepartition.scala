package utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random
import BaseUtil._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/10/29
  * Time: 9:15 
  * Description: 读取HDFS文件并且将大表重分区。
  */
object BigTableRepartition {
    def main(args: Array[String]) {

        val spark = ConnectUtil.spark
        val hdfsRoot = "hdfs://172.20.32.163:8020"
        val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
        val files = hdfsUtil.list("/YXFK/compute/")

        //feikong6TableRepartition(hdfsRoot, files, spark)
        val(res1, time1) = getMethodRunTime(readAllFileSize(spark,hdfsRoot,files))
        println(time1)


        Thread.sleep(1000*60*10)
    }

    /**
      * 处理费控相关的6个表的重分区
      *
      * @param hdfsRoot
      * @param files
      * @param spark
      */
    def feikong6TableRepartition(hdfsRoot: String, files: Array[String], spark: SparkSession): Unit = {
        files.par.foreach(path => {
            //这里必须确定一下类型
            val strPath: String = path
            if (strPath != "/YXFK/compute/HS_DJWDMX" && strPath != "/YXFK/compute/open") {
                strPath match {
                    case "/YXFK/compute/KH_JLD" =>
                        //读取/YXFK/compute/KH_JLD表并且重新分区
                        val df = spark.read.parquet(hdfsRoot + path)
                        val df2 = df.withColumn("PARTITION", getJldPartition(col("GDDWBM")))
                        //这种模式是按照列名分区，但是每个列文件中有不少分区，是因为没有将不同分区的数据拉去到同一分区。
                        //df2.write.partitionBy("PARTITION").mode(SaveMode.Overwrite).parquet(hdfsRoot + "/temp_data/temp_lihaoran/")
                        df2.repartition(29, col("PARTITION")).write.partitionBy("PARTITION").mode(SaveMode.Overwrite)
                            .parquet(hdfsRoot + "/temp_data/temp_lihaoran/KH_JLD")

                    case "/YXFK/compute/LC_HBXXJL" =>
                        val df = spark.read.parquet(hdfsRoot + path)
                            .withColumn("PARTITION", getHbxxPartititon(col("GDDWBM")))
                        df.repartition(col("PARTITION")).write.partitionBy("PARTITION").mode(SaveMode.Overwrite)
                            .parquet(hdfsRoot + "/temp_data/temp_lihaoran/LC_HBXXJL")

                    case "/YXFK/compute/LC_YXDNBSS" =>
                        val df = spark.read.parquet(hdfsRoot + path).repartition(30).write.mode(SaveMode.Overwrite)
                            .parquet(hdfsRoot + "/temp_data/temp_lihaoran/LC_YXDNBSS")

                    case "/YXFK/compute/HS_BYQXX" =>
                        val df = spark.read.parquet(hdfsRoot + path).repartition(6).write.mode(SaveMode.Overwrite)
                            .parquet(hdfsRoot + "/temp_data/temp_lihaoran/HS_BYQXX")

                    case "/YXFK/compute/KH_JLDGX" =>
                        val df = spark.read.parquet(hdfsRoot + path).repartition(18).write.mode(SaveMode.Overwrite)
                            .parquet(hdfsRoot + "/temp_data/temp_lihaoran/KH_JLDGX")

                    case "/YXFK/compute/SB_YXDNB" =>
                        val df = spark.read.parquet(hdfsRoot + path)
                            .withColumn("PARTITION", getYxdnbPartititon(col("GDDWBM")))
                        df.repartition(col("PARTITION")).write.partitionBy("PARTITION").mode(SaveMode.Overwrite)
                            .parquet(hdfsRoot + "/temp_data/temp_lihaoran/SB_YXDNB")

                    case "/YXFK/compute/KH_YDKH" =>
                        val df = spark.read.parquet(hdfsRoot + path)
                            .withColumn("PARTITION", getYdkhPartititon(col("GDDWBM")))
                        df.repartition(col("PARTITION")).write.partitionBy("PARTITION").mode(SaveMode.Overwrite)
                            .parquet(hdfsRoot + "/temp_data/temp_lihaoran/KH_YDKH")
                    case _ =>
                        println(path + "：暂时不复制")
                }
            }
        })


    }


    /**
      * 读取所有文件大小，以及供电单位编码的前5位。
      *
      * @param spark
      * @param hdfsRoot
      * @param files
      */
    def readAllFileSize(spark: SparkSession, hdfsRoot: String, files: Array[String]): Unit = {
        println("文件个数：" + files.length)
        println("大于100万的表记录数=========")
        val list1: List[String] = List()
        //不用并行读取的原因是防止打印的时候混乱。
        for (filePath <- files) {
            //将不符合的路径排除掉
            if (filePath != "/YXFK/compute/HS_DJWDMX" && filePath != "/YXFK/compute/open") {
                val df = spark.read.parquet(hdfsRoot + filePath)
                val num = df.count()
                println(filePath + ":" + num + "条记录====================")  //每个文件的记录数
                if (num > 1000000) {
                    list1.:+(filePath)
                    // 对大于100万的数据进行处理
                    try {
                        // println("查询某列相同key的记录数==============")
                        // df.printKeyNums("DQBM")
                        // df.printKeyNums("GDDWBM")
                        // 供电单位编码(前五位)
                        val df2 = df.withColumn("SUBGDDWBM", getSubgddwbm(col("GDDWBM")))
                        df2.printKeyNums("SUBGDDWBM")
                    } catch {
                        case ex: AnalysisException =>
                            println(s"${filePath}没有xxxxx字段===")
                    }
                }else{
                    //小于100万的另做处理
                }
            }
        }
    }

    /**
      * 得到前五位的供电单位编码
      *
      * @return
      */
    def getSubgddwbm: UserDefinedFunction = {
        udf { (gddwbm: String) =>
            if (gddwbm == null || gddwbm.trim().length == 0 || gddwbm.toLowerCase() == "null" || gddwbm.endsWith("00")) {
                "80000"
            } else {
                var sub5 = ""
                if (gddwbm.startsWith("0")) {
                    // 注意还有以0开头的。。。
                    sub5 = gddwbm.substring(1, 6)
                } else {
                    sub5 = gddwbm.substring(0, 5)
                }
                sub5
            }
        }
    }

    def 写活方法失败(spark: SparkSession, hdfsRoot: String, files: Array[String]): Unit = {
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
            udf { (gddwbm: String, subgddwbm: String, num: Long) =>
                var res = ""
                if (num > 100000) {
                    val partititonNum = (num / 200000 + 1).toInt
                    val gddwbmlist = List[String]()
                    for (i <- 1 to partititonNum) {
                        gddwbmlist.+:(subgddwbm + "_" + i)
                    }
                    println(gddwbmlist.length)
                    val ran = scala.util.Random.nextInt(partititonNum)
                    try {
                        res = gddwbmlist(ran)
                    } catch {
                        case ex: IndexOutOfBoundsException => {
                            println("数组角标越界====")
                            println(ran)
                            println(gddwbmlist.last)
                            println(gddwbmlist.indexOf(gddwbmlist.last))
                        }
                    }

                } else {
                    //小于10万的基本上没有
                    res = "800xx"
                }
                res
            }
        }
    }

    def getYdkhPartititon: UserDefinedFunction = {
        udf { (gddwbm: String) => {
            val sub5 =
                if (gddwbm == null || gddwbm.trim().length == 0 || gddwbm.toLowerCase() == "null" || gddwbm.endsWith("00")) {
                    "80000"
                } else {
                    var sub5 = ""
                    if (gddwbm.startsWith("0")) {
                        // 注意还有以0开头的。。。
                        sub5 = gddwbm.substring(1, 6)
                    } else {
                        sub5 = gddwbm.substring(0, 5)
                    }
                    sub5
                }
            val partition: String = sub5 match {
                case "80014" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80014A"
                        case 1 => "80014B"
                        case 2 => "80014C"
                    }
                }
                case "80011" => {
                    val ran = scala.util.Random.nextInt(5)
                    ran match {
                        case 0 => "80011A"
                        case 1 => "80011B"
                        case 2 => "80011C"
                        case 3 => "80011D"
                        case 4 => "80011E"
                    }
                }
                case "80006" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80006A"
                        case 1 => "80006B"
                        case 2 => "80006C"
                    }
                }
                case "80083" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80083A"
                        case 1 => "80083B"
                        case 2 => "80083C"
                    }
                }
                case "80013" => {
                    val ran = scala.util.Random.nextInt(4)
                    ran match {
                        case 0 => "80013A"
                        case 1 => "80013B"
                        case 2 => "80013C"
                        case 3 => "80013D"
                    }
                }
                case "80084" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80084A"
                        case 1 => "80084B"
                    }
                }
                case "80004" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80004A"
                        case 1 => "80004B"
                    }
                }
                case "80003" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80003A"
                        case 1 => "80003B"
                    }
                }
                case "80005" => {
                    val ran = scala.util.Random.nextInt(4)
                    ran match {
                        case 0 => "80005A"
                        case 1 => "80005B"
                        case 2 => "80005C"
                        case 3 => "80005D"
                    }
                }
                case "80015" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80015A"
                        case 1 => "80015B"
                    }
                }
                case "80012" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80012A"
                        case 1 => "80012B"
                    }
                }
                case _ => "800xx"
            }
            partition
            //不加会报错：ERROR ApplicationMaster: User class threw exception: java.lang.UnsupportedOperationException: Schema for type Unit is not supported
        }
        }
    }

    def getYxdnbPartititon: UserDefinedFunction = {
        udf { (gddwbm: String) => {
            val sub5 =
                if (gddwbm == null || gddwbm.trim().length == 0 || gddwbm.toLowerCase() == "null" || gddwbm.endsWith("00")) {
                    "80000"
                } else {
                    var sub5 = ""
                    if (gddwbm.startsWith("0")) {
                        // 注意还有以0开头的。。。
                        sub5 = gddwbm.substring(1, 6)
                    } else {
                        sub5 = gddwbm.substring(0, 5)
                    }
                    sub5
                }
            val partition: String = sub5 match {
                case "80014" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80014A"
                        case 1 => "80014B"
                        case 2 => "80014C"
                    }
                }
                case "80011" => {
                    val ran = scala.util.Random.nextInt(4)
                    ran match {
                        case 0 => "80011A"
                        case 1 => "80011B"
                        case 2 => "80011C"
                        case 3 => "80011D"
                    }
                }
                case "80006" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80006A"
                        case 1 => "80006B"
                        case 2 => "80006C"

                    }
                }
                case "80083" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80083A"
                        case 1 => "80083B"
                        case 2 => "80083C"
                    }
                }
                case "80013" => {
                    val ran = scala.util.Random.nextInt(4)
                    ran match {
                        case 0 => "80013A"
                        case 1 => "80013B"
                        case 2 => "80013C"
                        case 3 => "80013D"
                    }
                }
                case "80084" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80084A"
                        case 1 => "80084B"
                    }
                }
                case "80004" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80004A"
                        case 1 => "80004B"
                    }
                }
                case "80003" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80003A"
                        case 1 => "80003B"
                    }
                }
                case "80005" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80005A"
                        case 1 => "80005B"
                        case 2 => "80005C"
                    }
                }
                case "80015" => "80015"
                case "80012" => "80012"
                case _ => "800xx"
            }
            partition
        }
        }
    }

    def getHbxxPartititon: UserDefinedFunction = {
        udf { (gddwbm: String) => {
            val sub5 =
                if (gddwbm == null || gddwbm.trim().length == 0 || gddwbm.toLowerCase() == "null" || gddwbm.endsWith("00")) {
                    "80000"
                } else {
                    var sub5 = ""
                    if (gddwbm.startsWith("0")) {
                        // 注意还有以0开头的。。。
                        sub5 = gddwbm.substring(1, 6)
                    } else {
                        sub5 = gddwbm.substring(0, 5)
                    }
                    sub5
                }
            val partition: String = sub5 match {
                case "80014" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80014A"
                        case 1 => "80014B"
                        case 2 => "80014C"
                    }
                }
                case "80011" => {
                    val ran = scala.util.Random.nextInt(5)
                    ran match {
                        case 0 => "80011A"
                        case 1 => "80011B"
                        case 2 => "80011C"
                        case 3 => "80011D"
                        case 4 => "80011E"
                    }
                }
                case "80006" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80006A"
                        case 1 => "80006B"
                        case 2 => "80006C"

                    }
                }
                case "80083" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80083A"
                        case 1 => "80083B"
                        case 2 => "80083C"
                    }
                }
                case "80013" => {
                    val ran = scala.util.Random.nextInt(4)
                    ran match {
                        case 0 => "80013A"
                        case 1 => "80013B"
                        case 2 => "80013C"
                        case 3 => "80013D"
                    }
                }
                case "80084" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80084A"
                        case 1 => "80084B"
                    }
                }
                case "80004" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80004A"
                        case 1 => "80004B"
                        case 2 => "80004C"
                    }
                }
                case "80003" => {
                    val ran = scala.util.Random.nextInt(3)
                    ran match {
                        case 0 => "80003A"
                        case 1 => "80003B"
                        case 2 => "80003C"
                    }
                }
                case "80005" => {
                    val ran = scala.util.Random.nextInt(4)
                    ran match {
                        case 0 => "80005A"
                        case 1 => "80005B"
                        case 2 => "80005C"
                        case 3 => "80005D"
                    }
                }
                case "80015" => "80015"
                case "80012" => {
                    val ran = scala.util.Random.nextInt(2)
                    ran match {
                        case 0 => "80012A"
                        case 1 => "80012B"
                    }
                }
                case _ => "800xx"
            }
            partition
        }
        }
    }


    def getJldPartition: UserDefinedFunction = {
        udf { (gddwbm: String) => {
            val sub5 = if (!gddwbm.endsWith("00")) {
                gddwbm.substring(0, 5)
            } else {
                gddwbm
            }
            val partition: String = sub5 match {
                case "80014" => {
                    if (gddwbm == "80014" || gddwbm == "8001402" || gddwbm == "8001410" || gddwbm == "8001404"
                        || gddwbm == "8001405") {
                        //20万左右
                        "80014A"
                    } else if (gddwbm == "8001406" || gddwbm == "8001409" || gddwbm == "8001403") {
                        //10万
                        "80014B"
                    } else {
                        //这里可不可以用一个随机数？（0-2） 可以（已证实）
                        val ran = scala.util.Random.nextInt(3)
                        if (ran == 0) {
                            // 10万
                            "80014B"
                        } else {
                            // 20万
                            "80014C"
                        }
                    }
                }
                case "80011" => {
                    if (gddwbm == "8001102") {
                        "8001102" //20
                    } else if (gddwbm == "8001111" || gddwbm == "80011" || gddwbm == "8001114") {
                        "80011A" //22
                    } else if (gddwbm == "8001120") {
                        "80011B" //24
                    } else {
                        "80011C" //22
                    }
                }
                case "80006" => {
                    //共60万
                    val ran = scala.util.Random.nextInt(3)
                    if (ran == 0) {
                        "80006A"
                    } else if (ran == 1) {
                        "80006B"
                    } else {
                        "80006C"
                    }
                }
                case "80083" => {
                    //分成3分
                    if (gddwbm == "8008341" || gddwbm == "8008308" || gddwbm == "8008307") {
                        "80083A" //20
                    } else if (gddwbm == "8008309" || gddwbm == "8008317" || gddwbm == "8008306") {
                        "80083B" //19
                    } else {
                        "80083C" //28
                    }
                }
                case "80013" => {
                    //分成4分
                    if (gddwbm == "8001313" || gddwbm == "8001305" || gddwbm == "8001301") {
                        "80013A"
                    } else if (gddwbm == "8001314" || gddwbm == "8001306" || gddwbm == "8001310" || gddwbm == "8001307") {
                        "80013B"
                    } else if (gddwbm == "8001302" || gddwbm == "8001304") {
                        "80013C"
                    } else {
                        "80013D"
                    }
                }
                case "80084" => {
                    //31万
                    val ran = scala.util.Random.nextInt(2)
                    if (ran == 0) {
                        "80084A"
                    } else {
                        "80084B"
                    }
                }
                case "80004" => {
                    //49万
                    val ran = scala.util.Random.nextInt(2)
                    if (ran == 0) {
                        "80004A"
                    } else {
                        "80004B"
                    }
                }
                case "80003" => {
                    //46万
                    val ran = scala.util.Random.nextInt(2)
                    if (ran == 0) {
                        "80003A"
                    } else {
                        "80003B"
                    }
                }
                case "80005" => {
                    //72万
                    val ran = scala.util.Random.nextInt(3)
                    if (ran == 0) {
                        "80005A"
                    } else if (ran == 1) {
                        "80005B"
                    } else {
                        "80005C"
                    }
                }
                case "80015" => {
                    //29万
                    "80015"
                }
                case "80012" => {
                    //30万
                    "80012"
                }
                case _ => {
                    //2万
                    "800xx"
                }
            }
            partition
        }
        }
    }


}