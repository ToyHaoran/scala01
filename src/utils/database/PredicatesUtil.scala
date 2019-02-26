package utils.database

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * 用来得到spark.read.jdbc中的第三个参数Array(String),用来分区读取数据库，相当于where条件
  */
object PredicatesUtil {

  /**
    * 基于分类型字段获取分区，特别快，可以达到900个task。
    * KH_JSH数量：14923371,读取需要时间：40.16s
    * KH_JLD数量：12442418,读取需要时间：44.15s
    * SB_YXDNB数量：6129861,读取需要时间：70.71s
    * ZW_SSDFJL数量：13090988,读取需要时间：79.59s
    * KH_YDKH数量：22423943,读取需要时间：173.87s
    *
    * @param table  String
    * @param column String
    * @return
    */
  def byColumn(database: String, table: String, column: String, ignoreNull: Boolean): Array[String] = {
    /*
    整体逻辑：
    先读取数据库，得到列名数组，
    然后构造SQL语句。
     */
    val nullSql = s" $column IS NULL "
    //如果table位置是一个sql,肯定包含空格，只适用于单个表带条件的sql
    val finalTable = if (table.contains(" ")) {
      //使用正则表达式解析表名
      val matcher = "from [a-zA-Z0-9_]+".r
      matcher.findAllIn(table).next().split(" ").toList.last
    } else {
      table
    }
    //println(finalTable)
    val array = JdbcUtil.load(database, s"(SELECT DISTINCT $column FROM $finalTable) temp").collect()
        .map(x =>
          if (x.getAs[String](column) == null) {
            nullSql
          } else {
            s" $column ='${x.getAs[String](column)}' "
          })
    if (ignoreNull) array.filter(!_.equals(nullSql)) else array
  }

  /**
    * 将近若干天的数据进行分区读取（从endDate往前算）
    * 每一天作为一个分区，例如
    * Array(
    * "2015-09-17" -> "2015-09-18",
    * "2015-09-18" -> "2015-09-19",
    * ...)
    *
    * @param column  时间分区列
    * @param endDate 结束时间（或者今天时间）
    * @param t       时间间隔：多少天作为一个分区的周期
    * @param times   多少个分区：周期数。
    */
  def byDate(column: String, endDate: java.sql.Date, t: Int, times: Int): Array[String] = {
    /*
    整体逻辑：
    先构造ArrayByffer[(String, String)]()数组，将分段的起止时间加入，
    然后map构建Sql数组。
     */
    val cal = Calendar.getInstance()
    cal.setTime(endDate)
    cal.add(Calendar.DATE, -(times * t)) // 将当前日期减去（分区*时间间隔）
    val start2endBuffer = new ArrayBuffer[(String, String)]()
    (0 until times).foreach(_ => {
      val start = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      cal.add(Calendar.DATE, t) //当前日期加时间间隔
      val end = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      start2endBuffer.append(start -> end)
    })
    start2endBuffer.map { case (start, end) =>
      s"$column >= to_date('$start','yyyy-MM-dd') AND $column < to_date('$end','yyyy-MM-dd')"
    }.toArray
  }

  /**
    * 通过随机数加载数据库表
    *
    * @param column 已经处理好的随机数
    * @return
    */
  def byRandom(column: String, numPartition: Int): Array[String] = {
    (1 to numPartition).map(x => s" $column ='$x' ").toArray
  }


}
