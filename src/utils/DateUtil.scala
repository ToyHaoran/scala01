package utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}
import java.sql.Timestamp
/**
  * 操作日期工具类
  *
  */
object DateUtil {
  /**
    * 日期date转化为日期str
    */
  def dateToStr(srcDate: Date, pattern: String = "yyyy-MM-dd HH:mm:ss"): String = {
    if (srcDate == null) {
      null
    } else {
      val dateFormat: SimpleDateFormat = new SimpleDateFormat(pattern)
      val dateStr = dateFormat.format(srcDate)
      dateStr
    }
  }

  /**
    * 日期str转换为毫秒值str
    */
  def dateStrToMillStr(srcTime: String, pattern: String): String = {
    String.valueOf(dateStrToMill(srcTime, pattern))
  }

  /**
    * 日期str转化为毫秒值Long
    *
    * @param srcTime
    * @param pattern
    * @return
    */
  def dateStrToMill(srcTime: String, pattern: String): Long = {
    val date = dateStrToDate(srcTime, pattern)
    val ts = date.getTime()
    ts
  }

  /**
    * 日期str转化为date类型
    */
  def dateStrToDate(srcTime: String, pattern: String): Date = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(pattern)
    val date = dateFormat.parse(srcTime)
    date
  }

  /**
    * 将2018-08-09 00:00:00之类的字符串转为Timestamp，可以省略后半部分.
    * 常用来过滤DF中的操作时间。
    */
  def dateStrToTimestamp(srcTime: String):Timestamp={
    if(srcTime.contains(" ")){
      Timestamp.valueOf(srcTime)
    }else{
      Timestamp.valueOf(srcTime + " 00:00:00")
    }
  }


  /**
    * 日期字符串转化为另一种格式的字符串
    *
    * @param src
    * @param srcPattern
    * @param desPattern
    * @return
    */
  def dateStrToOtherStr(src: String, srcPattern: String, desPattern: String): String = {
    val date = dateStrToDate(src, srcPattern)
    val res = dateToStr(date, desPattern)
    res
  }

  /**
    * 获取当前年份
    */
  def getCurrentYear(): Int = {
    Calendar.getInstance().get(Calendar.YEAR)
  }

  /**
    * 计算相隔的天数
    */
  def getIntervalDays(startday: Calendar, endday: Calendar): Long = {
    var star = startday
    var end = endday
    //确保star在end之前
    if (star.after(end)) {
      val cal = star
      star = end
      end = cal
    }
    val d1 = star.getTimeInMillis
    val d2 = end.getTimeInMillis
    val minus = (d2 - d1) / 1000d / 60d / 60d / 24d
    minus.round
  }


  /**
    * 向前或向后推 N 个月
    */
  def addMonth(srcTime: Long, nMonth: Integer): Date = {
    addMonth(new Date(srcTime), nMonth)
  }

  /**
    * 向前或向后推 N 个月
    */
  def addMonth(srcTime: Date, nMonth: Integer): Date = {
    val time = addCalendar(srcTime.getTime, Calendar.MONTH, nMonth)
    new Date(time)
  }


  /**
    * 向前或向后推 N 分钟
    */
  def addMinute(srcTime: Date, nMonth: Integer): Date = {
    val time = addCalendar(srcTime.getTime, Calendar.MINUTE, nMonth)
    new Date(time)
  }


  /**
    * 向前或向后推 N 天
    */
  def addDay(srcTime: Long, nDay: Integer): Date = {
    addDay(new Date(srcTime), nDay)
  }

  /**
    * 向前或向后推 N 天
    */
  def addDay(srcTime: Date, nDay: Integer): Date = {
    val time = addCalendar(srcTime.getTime, Calendar.DAY_OF_MONTH, nDay)
    new Date(time)
  }

  /**
    * 向前或向后推 N 周
    */
  def addWeek(srcTime: Long, nDay: Integer): Date = {
    addWeek(new Date(srcTime), nDay)
  }

  /**
    * 向前或向后推 N 周
    */
  def addWeek(srcTime: Date, nDay: Integer): Date = {
    val time = addCalendar(srcTime.getTime, Calendar.DAY_OF_WEEK, nDay)
    new Date(time)
  }

  /**
    * 当前第几季
    */
  def currQuarter(srcTime: Long): Int = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    val currentMonth = calendar.get(Calendar.MONTH) + 1
    if (currentMonth >= 1 && currentMonth <= 3)
      1
    else if (currentMonth >= 4 && currentMonth <= 6)
      2
    else if (currentMonth >= 7 && currentMonth <= 9)
      3
    else if (currentMonth >= 10 && currentMonth <= 12)
      4
    else
      0
  }

  /**
    * 当季开始月
    */
  def currQuarterOfMonthBeg(srcTime: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    val year = calendar.get(Calendar.YEAR)
    val day = calendar.get(Calendar.DAY_OF_MONTH)
    val n = currQuarter(srcTime)
    n match {
      case 1 =>
        calendar.set(year, 1 - 1, day)
      case 2 =>
        calendar.set(year, 4 - 1, day)
      case 3 =>
        calendar.set(year, 7 - 1, day)
      case 4 =>
        calendar.set(year, 10 - 1, day)
    }
    calendar.getTimeInMillis
  }

  /**
    * 当季结束月
    */
  def currQuarterOfMonthEnd(srcTime: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    val year = calendar.get(Calendar.YEAR)
    val day = calendar.get(Calendar.DAY_OF_MONTH)
    val n = currQuarter(srcTime)
    n match {
      case 1 =>
        calendar.set(year, 3 - 1, day)
      case 2 =>
        calendar.set(year, 6 - 1, day)
      case 3 =>
        calendar.set(year, 9 - 1, day)
      case 4 =>
        calendar.set(year, 12 - 1, day)
    }
    calendar.getTimeInMillis
  }


  private def addCalendar(srcTime: Long, calendarField: Integer, nAmount: Integer): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    calendar.add(calendarField, nAmount)
    calendar.getTimeInMillis()
  }

  /**
    * 当前日期， 当月第一天
    */
  def firstDayOfMonth(srcTime: Long): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    new Date(calendar.getTimeInMillis())
  }

  /**
    * 当前日期，当年第一月第一天
    */
  def firstDayOfYear(srcTime: Long): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    calendar.set(Calendar.DAY_OF_YEAR, 1)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    new Date(calendar.getTimeInMillis())
  }


  def lastDayOfMonth(srcTime: Long): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH))
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    new Date(calendar.getTimeInMillis())
  }

  /**
    * 一天开始时间    00:00:00.0
    */
  def startTimeOfDay(srcTime: Date): Date = {
    startTimeOfDay(srcTime.getTime)
  }

  /**
    * 一天开始时间    00:00:00.0
    */
  def startTimeOfDay(srcTime: Long): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    val time = calendar.getTimeInMillis()
    new Date(time)
  }

  /**
    * time1 是否在 time2 之前
    */
  def beforeOfDay(time1: Date, time2: Date): Boolean = {
    time1.before(time2)
  }

  /**
    * time1是否在 time2之后
    */
  def afterOfDay(time1: Date, time2: Date): Boolean = {
    time1.after(time2)
  }

  /**
    * 是否同一天
    */
  def isSameDay(time1: Date, time2: Date): Boolean = {
    val cal1 = Calendar.getInstance()
    cal1.setTime(time1)
    val cal2 = Calendar.getInstance()
    cal2.setTime(time2)
    (cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA) && cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR)
        && cal1.get(Calendar.DAY_OF_YEAR) == cal2.get(Calendar.DAY_OF_YEAR))
  }

  /**
    * 今天周几<br>
    * 0 表示周日
    */
  def getWeekOfDay(srcTime: Long): Int = {
    val weekDays = Array[Int](0, 1, 2, 3, 4, 5, 6)
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(srcTime)
    val idx = calendar.get(Calendar.DAY_OF_WEEK) - 1
    weekDays(idx)
  }

  /**
    * 获取传入日期的前一天日期
    *
    * @param date 日期
    * @return
    */
  def getYesterday(date: Date): Date = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    cal.getTime
  }

}