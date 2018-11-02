package sparkdemo.practice

//现有userindo.txt，内有若干行用户数据，每一行代表一条用户数据包括两个字段，
// 第一个字段为用户编号，第二个字段为用户事件触发时间，监控程序监控用户动作，监控开始后每一分钟生成一条记录，
// 用户每间隔一分钟响应一次的话为持续响应，中间超过1分钟即断开，后续记录为第二次持续响应。
//1、统计用户的每个阶段连续响应时间，如果一个用户多次中断那么将产生多条统计数据；
//2、查找并输出每个用户的最长持续阶段的相应信息。
//说明：输出格式为用户编号,开始时间-结束时间,持续分钟数，如：
//A,2018-01-01 00:11:00-2018-01-01 00:18:00,7


import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Demo02 extends App{
    val sc = new SparkContext(new SparkConf().setAppName("Exam04").setMaster("local[2]"))
    val spark = SparkSession.builder().appName("Exam04").config("spark.some.config.option", "some-value").getOrCreate()
    import spark.implicits._

    //  val df = spark.read.textFile("source/userindo.txt").map(x=>x.split(",")).map(x=>(x(0),x(1))).toDF("id","time")

    //测试用
    val df = spark.read.textFile("F:\\桌面\\spark\\lihaoran\\source\\userindo.txt").map(x=>x.split(",")).map(x=>(x(0),x(1))).toDF("id","time")

    //思路；分组后进行时间统计成数组，然后后者减去前者的分钟，如果不为1，说明中断。
    //
    //  df.groupByKey(row=>row.getString(0)).mapGroups((id,groups)=>{
    //    //格式化日期
    //    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//A,2018-08-18 00:06:00
    //    //用来存放时间的数组
    //    var arr = new ArrayBuffer[Long]()
    //    //将时间保存到数组
    //    for(i<-groups){
    //      arr += fm.parse(i.getString(1)).getTime
    //    }
    //    arr.sorted//排序
    //
    //    var res = " "
    //    val length = arr.length
    //
    //    var star = ""
    //    var end = ""
    //    //持续分钟
    //    var min = 0
    //    for(i<-0 to length-2){
    //      val j = i+1
    //      //求持续时间间隔
    //      val minus = (fm.parse(arr(j)).getTime-fm.parse(arr(i)).getTime)/60000
    //      if(i==1){
    //        star = arr(i)
    //      }
    //      if(j==length-1){
    //        end = arr(j)
    //      }
    //      //两两相减，为1不处理，不为1记录
    //      if(minus==1){
    //        min + 1//持续分钟
    //      }else{
    //        end = arr(j)
    //        res += star + "-"+ end +","+min+" "
    //        //初始化min和star
    //        min = 0
    //        if(j!=length-1){
    //          star = arr(j+1)
    //        }
    //      }
    //    }
    //    (id,res)
    //  }).toDF("ID","INTERVAL")

    //
    val txtData = sc.textFile("F:\\桌面\\API\\Scala\\SparkDemo1\\source\\userindo.txt").map(_.split(",")).map(a => {
        val dateTimeNumber = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(a(1)).getTime
        (a(0), dateTimeNumber)//编码和毫秒值
    })

    //注意mapvalue和flatMapValue的区别
    val resultRDD = txtData.groupByKey().flatMapValues( values => {
        val list = values.toList.sorted//是否需要排序？
        var result = scala.collection.mutable.HashSet[(Long, Long)]()
        //第一种算法：使用两个控制变量
        var start = list(0)
        var pre = list(0)
        val duration =  60 * 1000 //表示一分钟
        for (i <- 1 to values.size - 1) {
            if (list(i) - pre == duration) {//如果没有中断
                pre = list(i)
                //这里也可以记个数，求持续时间。然后再中断之后把计数清零。也可以最后再处理
            } else {
                //如果中断，说明pre是上一个阶段的end，返回（star，end）
                result.+=((start, pre))
                //重置start和pre
                start = list(i)
                pre = list(i)
            }
        }
        result.+=((start, pre))//最后把没有中断的最后一个加上，(因为有可能只剩一个，所以有可能是相同的值，处理成间隔为0)
        result
        //    //第二种算法：后者减前者，需要加一些判断。判断太多，舍弃。
        //需要判断数组长度是否为1，加上
        //    var star = list(0)
        //    var end = list(1)
        //    for(i <- 0 to values.size-2){
        //      val j = i+1
        //      if(end-star == duration){//这里也要判断一下j是否是最后一个。是的话end应该等于list(j)
        //        end = list(i)
        //      }else{//此次i，j之间断开
        //
        //        result.+=((star,end))
        //        star = list(j)
        //        //最后一次怎么处理？？判断一下j是否是最后一个，如果是，end和star是同一个值，如果不是，正常情况。
        //        end =list(j+1)
        //      }
        //    }
        //    result
    })
    val resultDf = resultRDD.map(ele => {
        val startLong = ele._2._1 - 60*1000//因为第一个数据是一分钟后统计的。
        val endLong = ele._2._2
        val duration = (endLong-startLong) / (60*1000)//求持续时间
        (ele._1.toString,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startLong)),//看看这个的源文件，是不是互相转化的。
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(endLong)),
            duration.toString())
    })
    println(resultDf.collect().mkString(","))
    sc.stop()



}