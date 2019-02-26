package utils.DataIntegration

import java.text.SimpleDateFormat

import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.PropUtil
import utils.database.JdbcUtil

import scala.util.{Failure, Success, Try}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2019/1/7
  * Time: 11:55 
  * Description: 数据集成中的初始化。最好能够抽取出来，所以耦合度要低。
  */
object DataInit {

  private final val database = PropUtil.getValueByKey("DATAINIT.DATABASE")
  val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT")
  //默认分区数
  val defaultPartitions: Int = PropUtil.getValueByKey("DEFAULTPARTITION").toInt
  //需要分区的字段
  val columnPartition = PropUtil.getValueByKey("COLUMNPARTITION")
  //当前时间
  final val CURRENT_DATE = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date())
      .replace(" ", "_").replace(":", "-")
  //初始化表的保存路径，如：hdfs://10.150.80.103:8020/YXFK/compute
  val computePath: String = hdfsRoot + PropUtil.getValueByKey("COMPUTEPATH")
  val incrementPath: String = hdfsRoot + PropUtil.getValueByKey("INCREMENTPATH")

  //测试
  def main(args: Array[String]): Unit = {
    //bug1:一直权限不对:Permission denied: user=root, access=WRITE, inode="/datatest/compute/DW_YXBYQ/_temporary/0":llhr:hdfs:drwxr-xr-x,
    // 一般是HDFS地址没写对，是不是指向了另一个地址？
    initData()
  }

  def initData(): Unit = {
    (0 to 1).par.foreach {
      case 0 =>
        //小表直接单线程并行读取
        PropUtil.getValueByKey("SMALL.TABLE").split(",").par.foreach(table =>
          Try {
            JdbcUtil.load(database, table).write.mode(SaveMode.Overwrite).parquet(s"$computePath/$table")
          } match {
            case Success(r) =>
              println(s"${table}初始化成功====")
            case Failure(e) =>
              println(s"${table}初始化失败=========================")
              println(e.printStackTrace())
          }
        )
      case 1 =>
        //大表并行读取
        PropUtil.getValueByKey("DATAINIT.TABLES").split(",").par.foreach(table =>
          Try {
            //加载数据库表，已cache
            val data = JdbcUtil.loadTable(database, table, columnPartition, defaultPartitions)
            Array(computePath, incrementPath).par.foreach(path => {
              val childPath =
                if (path.equals(incrementPath)) {
                  //这个备份没用可以删掉。
                  s"$path/${CURRENT_DATE}_INIT/$table"
                } else {
                  s"$path/$table"
                }
              data.write.mode(SaveMode.Overwrite).parquet(childPath)
              data.unpersist()
            })
          } match {
            case Success(r) =>
              println(s"${table}初始化成功====")
            case Failure(e) =>
              println(s"${table}初始化失败=============================")
              println(e.printStackTrace())
          }
        )
    }
  }
}