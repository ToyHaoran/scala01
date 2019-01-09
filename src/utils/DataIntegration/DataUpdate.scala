package utils.DataIntegration

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.concurrent.ConcurrentHashMap

import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import utils.database.JdbcUtil
import utils.{ConnectUtil, DateUtil, HDFSUtil, PropUtil}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2019/1/7
  * Time: 15:56 
  * Description: 增量同步通用化
  */
object DataUpdate {
  private final val database = PropUtil.getValueByKey("DATAINIT.DATABASE")
  val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT")
  //默认分区数
  val defaultPartitions: Int = PropUtil.getValueByKey("DEFAULTPARTITION").toInt
  //需要分区的字段
  val columnPartition = PropUtil.getValueByKey("COLUMNPARTITION")
  //当前时间
  final val CURRENT_DATE = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date())
      .replace(" ", "_").replace(":", "-")
  private final val CURRENT_DAY = new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
  val computePath: String = hdfsRoot + PropUtil.getValueByKey("COMPUTEPATH")
  val incrementPath: String = hdfsRoot + PropUtil.getValueByKey("INCREMENTPATH")
  private final val INCREMENT_BASE_PATH: String = s"$incrementPath/${CURRENT_DAY}_UPDATE/"
  val pathMap: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()
  val hdfsUtil = new HDFSUtil(hdfsRoot)
  val partitioner = new HashPartitioner(100)
  //费控结果数据库库
  val fkresult = "fkresult"


  def main(args: Array[String]) {
    println("开始进行增量同步==========================")
    //获取增量同步数据库
    val database = PropUtil.getValueByKey("DATASYNC.DATABASE")
    //获取增量同步表
    val tables = PropUtil.getValueByKey("DATAINIT.TABLES")
    val tupleIsNull = (x: (Any, Any)) => x._1 == null || x._2 == null
    Array((database, tables))
        .filter(!tupleIsNull(_))
        .foreach(x => sync(x._1, x._2.split(",")))
    println("增量同步完成=============================")
  }

  /**
    * 默认更新前一天的数据。
    */
  def sync(database: String, tables: Array[String]): Unit = {
    //增量数据备份
    val tables_data: Array[(String, DataFrame)] = incrementalBackup(database, tables)
    //落入Mysql
    val mysqlTables = PropUtil.getValueByKey("FEIKONG.DATAUPDATE.MYSQL.TABLES").split(",")
    tables_data.filter(x => mysqlTables.contains(x._1))
        .foreach(t => JdbcUtil.update("fkresult", t._1, t._2))
    //增量数据同步
    tables_data.foreach(x => x._2.show(false))
    incrementalUpdates(database, tables_data)
    //小表直接保存
    PropUtil.getValueByKey("FEIKONG.SMALL.TABLE").split(",").par.foreach(x =>
      JdbcUtil.load(database, "" + x + "").write.mode(SaveMode.Overwrite).parquet("/YXFK/compute/" + x)
    )
  }

  /**
    * 传入起止时间，获得这段时间之内的增量数据。
    */
  def syncRQ(database: String, tables: Array[String], starttime: String, endtime: String): Unit = {
    (0 to 1).par.foreach {
      case 0 =>
        //小表直接保存(load并行，par并行数据量小的情况下，到底哪个快，感觉时间差不多，未测试)
        PropUtil.getValueByKey("SMALL.TABLE").split(",").par.foreach(table =>
          JdbcUtil.load(database, table).write.mode(SaveMode.Overwrite).parquet(s"$computePath/$table")
        )
      case 1 =>
        //获取增量数据并备份
        val tables_data: Array[(String, DataFrame)] = {
          //重算时间
          val cssj = endtime.replace("-", "").substring(0, 7)
          //加载数据库指定时间的数据，并过滤掉没有数据的DF
          val table_data = tables.par.map(table => (table, {
            //todo 这里能不能加随机数？？这里要改。
            val sql = new StringBuilder(
              s"SELECT * FROM $table WHERE CZSJ > to_date('$starttime','yyyy-mm-dd hh24:mi:ss') " +
                  s"AND CZSJ <= to_date('$endtime','yyyy-mm-dd hh24:mi:ss') ")
            println("########" + sql.toString())
            val finalSql = s"(${sql.toString()})"
            //这里感觉没必要并行读取，上面表已经并行了，任务量差不多，大致也能充分利用线程。就怕某个表突然更新了很多数据。
            //加上并行还多出来去重的时间以及大量的任务，还不如去掉。（未测试）
            JdbcUtil.load(database, finalSql)
          })).filter(_._2.take(1).nonEmpty)
          //将数据备份到增量目录，以防更新失败二次更新。
          table_data.foreach(x => {
            val increment = x._2
            increment.write.mode(SaveMode.Overwrite).parquet(s"$incrementPath${cssj}_UPDATE/${x._1}")
          })
          //返回 Array((tableName：DataFrame)....)
          table_data.toArray
        }

        //将指定的表落入Mysql
        val mysqlTables = PropUtil.getValueByKey("MYSQL.TABLES").split(",")
        tables_data.filter(x => mysqlTables.contains(x._1)).foreach(t => JdbcUtil.update(fkresult, t._1, t._2))

        //打印几行看看数据
        tables_data.foreach(x => x._2.show(false))
        //开始增量数据同步----------
        //对增量数据和原始数据进行join，不需要修改。不会出现数据覆盖的情况。
        //覆盖的时候，会临时出现当天的日期备份，不用管，一会就删除。
        tables_data.par.foreach(x => updateData(database, x._2, x._1))

        Logger.getLogger("org.apache.spark").warn("数据更新完成,正在覆盖目录 ...")
        //Parquet写入成功,修改全部覆写路径
        import scala.collection.JavaConversions._
        pathMap.keySet().par.foreach(
          x => {
            hdfsUtil.delete(pathMap(x))
            hdfsUtil.rename(x, pathMap(x))
          }
        )
        Logger.getLogger("org.apache.spark").warn("目录覆盖完成,SUCCESS!")
      //大表增量同步完成--------------
    }
  }

  /**
    * 增量数据同步
    *
    * @param tables_data Array[表名,DataFrame]
    */
  def incrementalUpdates(database: String, tables_data: Array[(String, DataFrame)]): Unit = {
    tables_data.par.foreach(x => updateData(database, x._2, x._1))
    Logger.getLogger("org.apache.spark").warn("数据更新完成,正在覆盖目录 ...")
    //Parquet写入成功,修改全部覆写路径
    import scala.collection.JavaConversions._
    pathMap.keySet().par.foreach(
      x => {
        hdfsUtil.delete(pathMap(x))
        hdfsUtil.rename(x, pathMap(x))
      }
    )
    Logger.getLogger("org.apache.spark").warn("目录覆盖完成,SUCCESS!")
  }

  /**
    *
    * 对增量数据单独进行备份，写到增量备份目录下
    * 增量备份全部完成后才进行增量更新
    * 若增量更新报错后重新执行程序，若有当天的增量备份可直接读取当天增量parquet
    *
    * @param tables 增量备份的表
    * @return
    */
  def incrementalBackup(database: String, tables: Array[String]): Array[(String, DataFrame)] = {
    val cal = Calendar.getInstance()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dqrq = dateFormat.format(cal.getTime)
    val testDate = DateUtil.dateStrToDate(dqrq, "yyyy-MM-dd")
    val table_data = tables.par.map(x => (x, loadUpdateData(database, x, testDate, "031800"))) //后面这个DQBM并没有用，删除也可以。
        .filter(_._2.take(1).nonEmpty)
    table_data.foreach(
      x => {
        val increment = x._2
        increment.write.mode(SaveMode.Overwrite).parquet(s"$INCREMENT_BASE_PATH${x._1}")
      }
    )
    table_data.toArray
  }

  /**
    * 根据操作时间自动增量同步。
    * 1. 读取增量更新的数据，根据操作时间判断，取操作时间在传入的时间的前一天范围内的数据：
    * 传入 2018-06-19 ，范围为 CZSJ>2018-06-18 00:00:00 AND CZSJ <= 2018-06-18 23:59:59
    * 2. 若前一天为执行初始化日期，取初始化时间作为开始区间
    *
    * @param table    增量更新表
    * @param date     当前日期
    * @param area     是否过滤，如需过滤传入字段编码值
    * @param areaCode 过滤字段，默认根据地区编码DQBM过滤
    * @return
    */
  def loadUpdateData(database: String, table: String, date: Date, area: String = "", areaCode: String = ""): DataFrame = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val preDate = DateUtil.getYesterday(date)
    val dateStr = sdf.format(preDate)
    val sourceDate = hdfsUtil.listDirs(incrementPath)
        .map(_.split("/").last)
        .filter(x => x.split("_").last.equals("SOURCE"))
        .maxBy(x =>
          x.replace("-", "")
              .replace(";", "")
              .split("_").take(2).mkString("").toLong)
    val (sourceDay, sourceTime) = {
      val splits = sourceDate.split("_")
      (splits(0), splits(1).replace(";", ":"))
    }
    val startTime = s"'$dateStr 00:00:00'"
    val endTime = s"'$dateStr 23:59:59'"

    val sql = new StringBuilder(
      s"SELECT * FROM $table WHERE CZSJ > to_date($startTime,'yyyy-mm-dd hh24:mi:ss') " +
          s"AND CZSJ <= to_date($endTime,'yyyy-mm-dd hh24:mi:ss') ")
    //    if (!area.equals(""))
    //      sql.append(s"AND $areaCode ='$area' ")
    println("########" + sql.toString())
    val finalSql = s"(${sql.toString()})"
    JdbcUtil.load(database, finalSql)
  }


  /**
    * 1。根据数据主键进行数据更新
    * 2。如果有供电单位编码字段根据供电单位编码并行更新，对每个供电单位编码数据过滤并行处理（废弃）
    * 3。主键通过提前配置对InitTableMap中对keyColumnMap中获取
    *
    * @param dataUpdate 需被同步的数据
    * @param table      表名
    */
  def updateData(database: String, dataUpdate: DataFrame, table: String): Unit = {
    val keysOpt: Array[String] = PropUtil.getPrimaryKeys(table)
    require(keysOpt.nonEmpty, "")
    val sourcePath = s"$computePath/$table"
    //原始数据
    val sourcedf = ConnectUtil.spark.read.parquet(sourcePath)
    val df1 = sourcedf.withColumn("FLAG", lit("1"))
    val columnName = sourcedf.columns
    val df2 = dataUpdate.withColumn("FlAG", lit("1"))
    //根据主键个数，获取连接条件
    def getCondition(df1: DataFrame, df2: DataFrame, keysOpt: Array[String]): DataFrame = {
      val keysSize = keysOpt.length
      val joinType = "fullouter"
      keysSize match {
        case 1 =>
          val key1 = keysOpt(0)
          df1.join(df2, df1(key1) === df2(key1), joinType)
        case 2 =>
          val (key1, key2) = (keysOpt(0), keysOpt(1))
          df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2), joinType)
        case 3 =>
          val (key1, key2, key3) = (keysOpt(0), keysOpt(1), keysOpt(2))
          df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2) && df1(key3) === df2(key3), joinType)
        case 4 =>
          val (key1, key2, key3, key4) = (keysOpt(0), keysOpt(1), keysOpt(2), keysOpt(3))
          df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2) && df1(key3) === df2(key3) &&
              df1(key4) === df2(key4), joinType)
        case 5 =>
          val (key1, key2, key3, key4, key5) = (keysOpt(0), keysOpt(1), keysOpt(2), keysOpt(3), keysOpt(4))
          df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2) && df1(key3) === df2(key3) &&
              df1(key4) === df2(key4) && df1(key5) === df2(key5), joinType)
        case 6 =>
          val (key1, key2, key3, key4, key5, key6) = (keysOpt(0), keysOpt(1), keysOpt(2), keysOpt(3),
              keysOpt(4), keysOpt(5))
          df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2) && df1(key3) === df2(key3) &&
              df1(key4) === df2(key4) && df1(key5) === df2(key5) && df1(key6) === df2(key6), joinType)
        case 7 =>
          val (key1, key2, key3, key4, key5, key6, key7) = (keysOpt(0), keysOpt(1), keysOpt(2), keysOpt(3),
              keysOpt(4), keysOpt(5), keysOpt(6))
          df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2) && df1(key3) === df2(key3) &&
              df1(key4) === df2(key4) && df1(key5) === df2(key5) && df1(key6) === df2(key6)
              && df1(key7) === df2(key7), joinType)
      }
    }
    val df3 = getCondition(df1, df2, keysOpt)
    val df4 = df3.filter(df2("FLAG") === "1").select(columnName.map(c => df2(c)): _*)
    val df5 = df3.filter(df2("FLAG").isNull).select(columnName.map(c => df1(c)): _*)
    val union_Data = df4.union(df5).drop("FLAG")
    overwrite(union_Data, sourcePath)
  }

  /**
    * 1。对合并后更新后的数据写parquet
    * 2。根据对于路径生成临时路径，写入到临时路径中，防止可能的报错污染原始数据
    *
    * @param dataFrame 落盘数据
    * @param path      数据对应写入路径
    */
  def overwrite(dataFrame: DataFrame, path: String): Unit = {
    val now = new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
    val temporaryPath = s"$path@$now" //临时目录
    dataFrame.write.mode(SaveMode.Overwrite).parquet(temporaryPath)
    pathMap.put(temporaryPath, path)
  }

}