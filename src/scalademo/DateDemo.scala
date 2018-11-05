package scalademo

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import utils.BaseUtil.int2boolen
import utils.DateUtils._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/5
  * Time: 16:05 
  * Description:
  */
object DateDemo extends App{
    //参考DataUtils工具类

    val date操作 = 0
    if(1){
        //仅供参考，实际开发还是需要写基础步骤

        val now: Date = new Date()
        println("得到毫秒值====")
        println(now.getTime)

        println("毫秒值转化为日期=====")
        val date3 = new Date(1458128053000L)
        println(date3)

        println("Str转化为Date=====")
        println(dateStrToDate("2016-03-16 19:34:13","yyyy-MM-dd HH:mm:ss"))

        println("Date转化为Str=====")
        println(dateToStr(now, "yyyy-MM-dd"))

        println("Str转化为毫秒值Long=====")
        println(dateStrToMill("2016-03-16 19:34:13","yyyy-MM-dd HH:mm:ss"))

        println("Str转化为毫秒值str======")
        println(dateStrToMillStr("2016-03-16 19:34:13","yyyy-MM-dd HH:mm:ss"))

        println("字符串转化为另一种格式的字符串========")
        println(dateStrToOtherStr("2016-03-16 19:34:13","yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd"))
    }


    val 日期转化为字符串 = 0
    if(0){

        val dateString = "2016-03-16 19:34:13"
        //格式化date
        //  val loc = new Locale("en")//时间格式改为本地
        //  val fm = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"),loc)
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = format.parse(dateString)
        //将毫秒值转化为日期
    }

    val 字符串转化为日期 = 0
    if(0){

    }

    val 计算时间间隔 = 0




    if(0){
        //——————————————————————对日期的操作————————————————————————————————————
        val dateString = "2016-03-16 19:34:13"
        //格式化date
        //  val loc = new Locale("en")//时间格式改为本地
        //  val fm = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"),loc)
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = format.parse(dateString)
        //将毫秒值转化为日期

        //Date操作
        date.getTime//距1970年，得到毫秒值
        // 以下全部过期
        date.getYear //距离1900年的间隔，为116
        date.getMonth+1
        date.getDay
        date.getHours
        date.getMinutes
        date.getSeconds

        //推荐使用Calendar操作
        val calendar = Calendar.getInstance()
        //能进行自定义时间
        calendar.setTime(date)
        calendar.set(Calendar.YEAR,2016)
        //获取想要的数据
        calendar.get(Calendar.YEAR) //1999
        calendar.get(Calendar.MONTH)+1//注意month是从0开始的
        calendar.get(Calendar.WEEK_OF_YEAR)//一年中的第几周
        calendar.get(Calendar.WEEK_OF_MONTH)//一个月中的第几周
        calendar.get(Calendar.DAY_OF_YEAR)//一年的第几天
        calendar.get(Calendar.DAY_OF_MONTH)//一个月的第几天
        calendar.get(Calendar.DAY_OF_WEEK)-1//一周的第几天。week是从星期天开始的。
        calendar.get(Calendar.DAY_OF_WEEK_IN_MONTH)
        calendar.get(Calendar.HOUR_OF_DAY)//24小时制
        calendar.get(Calendar.MINUTE)
        calendar.getTimeInMillis//得到毫秒值
        calendar.getTime//得到date

        //常用方法示例
        calendar.getActualMaximum(Calendar.DAY_OF_MONTH)//计算本月的天数
        calendar.getActualMinimum(Calendar.MONTH)//0
        //计算今年第8个星期四是几月几号
        calendar.set(Calendar.WEEK_OF_YEAR,8)
        calendar.set(Calendar.DAY_OF_WEEK,5)
        format.format(calendar.getTime)//2016-02-18 19:34:13

        //对日期进行加减
        calendar.add(Calendar.DAY_OF_YEAR,5)//2016-02-23 19:34:13
        calendar.add(Calendar.DAY_OF_MONTH,-25)//2016-01-29 19:34:13
        //对数据进行加减，但是循环
        calendar.roll(Calendar.DAY_OF_MONTH,5)//2016-01-03 19:34:13//本月内进行循环
        calendar.roll(Calendar.DAY_OF_WEEK,7)//2016-01-03 19:34:13//本星期内进行循环

        //计算相隔的天数
        def getIntervalDays(startday: Calendar,endday: Calendar): Long ={
            var star = startday
            var end = endday
            //确保star在end之前
            if(star.after(end)){
                var cal = star
                star= end
                end = cal
            }
            val d1 = star.getTimeInMillis
            val d2 = end.getTimeInMillis
            val minus = (d2 - d1)/1000/60/60/24
            minus
        }
    }

    val 计算程序运行时间 = 0
    if(0){
        val (res, time) = getMethodRunTime({
            println("hello")
            Thread.sleep(1000)
        })
        print(time)
    }

}