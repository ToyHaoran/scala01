package utils.database

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Namhwik on 2018/7/6.
  */
object PredicatesUtil {

  /**
    * 基于分类型字段获取分区
    *
    * @param table  String
    * @param column String
    * @return
    */
  def predicates(database: String, table: String, column: String, ignoreNull: Boolean): Array[String] = {
    val nullSql = s" $column IS NULL "
    val array = JdbcUtil.load(database, s"(SELECT DISTINCT $column FROM $table)")
      .collect()
      .map(
        x =>
          if (x.getAs[String](column) == null)
            nullSql
          else
            s" $column ='${x.getAs[String](column)}' "
      )

    if (ignoreNull) array.filter(!_.equals(nullSql)) else array
  }

  /**
    * 将近若干天的数据进行分区读取
    * 每一天作为一个分区，例如
    * Array(
    * "2015-09-17" -> "2015-09-18",
    * "2015-09-18" -> "2015-09-19",
    * ...)
    **/
  def predicates(column: String, date: java.sql.Date, t: Int, times: Int): Array[String] = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -(times * t))
    val start2endBuffer = new ArrayBuffer[(String, String)]()
    (0 until times).foreach(_ => {
      val start = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      cal.add(Calendar.DATE, t)
      val end = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      start2endBuffer.append(start -> end)
    })
    start2endBuffer.map {
      case (start, end) =>
        s"$column >= to_date('$start','yyyy-MM-dd') AND $column < to_date('$end','yyyy-MM-dd')"
    }.toArray
  }
}
