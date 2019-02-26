package sparkdemo.test

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import utils.BaseUtil._
import utils.database.JdbcUtil
import utils.{ConnectUtil, HDFSUtil, PropUtil}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/16
  * Time: 10:54 
  * Description:
  */
object ReadParquet extends App {

  val 打印指定表key的记录数 = 0
  if (0) {
    val spark = ConnectUtil.spark
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.162")
    val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
    //val files = hdfsUtil.list("/YXFK/compute/")  //所有表
    //val tables = "KH_YDKH" //指定表
    val tables: String = args(0)
    val files = tables.split(",").map(s => "/YXFK/compute/" + s).toList.toArray
    //需要统计的列名
    val colName = "GDDWBM"
    colName.split(",").foreach(name => {
      println("要统计的列名==========" + colName + "===========================")
      println("文件个数：" + files.length)
      for (filePath <- files) {
        try {
          val df = spark.read.parquet(hdfsRoot + filePath)
          val num = df.count()
          if (num > 1000000) {
            if (num > 5000000) {
              println(filePath + ":" + num + "条记录，大于500万================================")
            } else {
              println(filePath + ":" + num + "条记录，大于100万====================")
            }
            try {
              // println("查询某列相同key的记录数==============")
              df.printKeyNums(colName)
            } catch {
              case ex: AnalysisException =>
                println(s"${filePath}没有${colName}字段===")
            }
          } else {
            println(filePath + ":" + num + "条记录=======")
            //小于100万的另做处理
          }
        } catch {
          case ex: AnalysisException =>
            println(s"${filePath}不是正常的parquet文件==")
        }
      }
      println("=======================================================================================")
    })
  }


  /*
  遇到的问题：
    1、单个Executor读取的时候被杀掉，内存溢出。
      解决：提高executor-memory，或者重新repartition。
   */
  val 处理所有parquet = 0
  if (0) {
    val spark = ConnectUtil.spark
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.103")
    val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
    //hdfsUtil.setUser("appuser")
    //val files = hdfsUtil.list("/YXFK/increment/20181126_UPDATE")
    //处理两个路径
    val files = hdfsUtil.list("/user/appuser/YXFK/YXFK/compute/") ++
        hdfsUtil.list("/user/appuser/YXFK/YXFK/increment/2018-11-23_11;27;00_SOURCE/")
    println("文件个数：" + files.length)
    files.par.foreach(filePath => {
      try {
        val df = spark.read.parquet(hdfsRoot + filePath)
        //有待测试
        if (df.count() > 1000000) {
          df.repartition(200).write.mode(SaveMode.Overwrite).parquet(hdfsRoot + filePath.replace("/user/appuser/YXFK", ""))
        }
        println(filePath + "复制完成==============")
      } catch {
        case ex: AnalysisException =>
          println(s"${filePath}不是正常的parquet文件,读取失败========")
      }
    })
    //不用并行读取的原因是防止打印的时候混乱。
    /*for (filePath <- files) {
      try {
        val df = spark.read.parquet(hdfsRoot + filePath)
        //对parquet进行处理
        df.repartition(200).write.mode(SaveMode.Overwrite).parquet(hdfsRoot + filePath.replace("/user/appuser/YXFK", ""))
        println(filePath + "复制完成==============")

        /*val num = df.count()
        println(filePath + ":" + num + "================================")
        df.printSchema()*/
        //df.show(5)
        //df.filter("CZSJ = '2018-11-27 19:10:55'").show()
        //df.printSchema()
      } catch {
        case ex: AnalysisException =>
          println(s"${filePath}不是正常的parquet文件,读取失败========")
        //println(s"${filePath}无此字段,读取失败========")
      }
    }*/

  }

  val 处理指定parquet = 0
  if (0) {
    val spark = ConnectUtil.spark
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.123")

    val tables = "FW_KFGDXX"
    val files = tables.split(",").map(s => "/YXFK/compute/" + s).toList.toArray

    for (filePath <- files) {
      try {
        val df = spark.read.parquet(hdfsRoot + filePath)
        val num = df.count()
        println(filePath + ":" + num + "================================")
        df.show()
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
  }

  val 打印指定表和字段的Schema = 0
  //目前的功能是打印DQBM,GDDWBM字段类型不是String的表。
  if (0) {
    val spark = ConnectUtil.spark
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.123")
    val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
    //val files = "FW_KFGDXX".split(",").map(s => "/YXFK/compute/" + s).toList.toArray  //指定表
    val files = hdfsUtil.list("/YXFK/compute/") //所有表
    val columns = "DQBM,GDDWBM" //指定字段
    println("文件个数：" + files.length + "====================")
    val errorTable = scala.collection.mutable.Set.empty[String]
    files.foreach(filePath => {
      try {
        val df = spark.read.parquet(hdfsRoot + filePath)
        println(filePath + "：记录总数：" + df.count() + "================================================================")
        df.show(5)
        columns.split(",").foreach(col => {
          try {
            val df1 = df.select(col)
            df1.printSchema()
            //对schema类型进行判断：如果不是string类型，说明表有问题。
            if (!df1.schema(0).dataType.typeName.equals("string")) {
              //将有错误的表保存一下
              errorTable += filePath.replace("/YXFK/compute/", "")
            }
          } catch {
            case ex: AnalysisException =>
              println(s"${filePath}无${col}字段,读取失败==========")
          }
        })
      } catch {
        case ex: AnalysisException =>
          println(s"${filePath}不是正常的parquet文件,读取失败=======")
      }
    })
    //打印有错误的表
    println(s"总共${errorTable.size}个有错误的表")
    println(errorTable.mkString(","))
  }


}