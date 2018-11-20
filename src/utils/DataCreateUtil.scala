package utils

import BaseUtil._
/**
  * Created with IntelliJ IDEA.
  * User: lihaoran
  * Date: 2018/11/1
  * Description: 用来创造数据
  */
object DataCreateUtil{

    def main(args: Array[String]): Unit = {
        val (res, time) = getMethodRunTime({
            textCreate(999999," ",lineSplit = true, perNum = 100)
        })
        println(res)
        println(time)

    }


    /**
      * 不分行、以及固定长度换行
      *
      * @param totalTimes 基础单词的总次数
      * @param sep 分隔符，默认","
      * @param lineSplit 是否换行，默认不换行
      * @param perNum 如果固定换行，每行的单词数，默认10
      */
    def textCreate(totalTimes: Int, sep: String = ",", lineSplit: Boolean = false, perNum: Int = 10, kind: String = "水果"): String = {
        //用来造的基础数据，比如说水果，基本单词
        val baseDataArray = getBaseData(kind)

        val len = baseDataArray.length
        // 99万数据大约是1.7到2.2秒
        // "并不需要并行计算，99万也可以在几秒内运行完"
        //用来保存数据的列表
        var lst = List[String]()
        for (i <- 1 to totalTimes) {
            // 基于索引的快速访问需要使用数组
            val ran = scala.util.Random.nextInt(len)
            val str = if (!lineSplit) {
                //不换行
                sep + baseDataArray(ran)
            } else {
                //固定次数换行，也可以用随机换行的方法
                if (i % perNum == 0) {
                    "\n" + baseDataArray(ran)
                } else {
                    sep + baseDataArray(ran)
                }
            }
            // 快速添加使用列表，注意是往头部添加的。
            lst = str :: lst
        }
        lst.mkString("")
    }

    def textCreate4Fruit(totalTimes: Int = 10000, sep: String = ","): String ={
        //"苹果", "梨", "橘子", "芭蕉"
        textCreate(totalTimes, sep, kind = "四种水果")
    }

    /**
      * 随机换行
      *
      * @param totalTimes 基础单词的总次数
      * @param sep 分隔符，默认","
      * @param star 随机换行最小值
      * @param end 随机换行最大值
      * @return
      */
    def textCreateRandomLine(totalTimes: Int, sep: String = ",", star: Int = 10, end: Int = 20): String = {
        //用来造的基础数据，比如说水果，基本单词
        val baseDataArray = getBaseData("水果")
        val len = baseDataArray.length
        //用来随机换行的变量
        var ranCreateFlag = true
        var ranCreated = 0
        val range = end - star + 1
        //用来保存数据的列表
        var lst = List[String]()
        for (i <- 1 to totalTimes) {
            // 基于索引的快速访问需要使用数组
            val ran = scala.util.Random.nextInt(len)
            val str = {
                /*思路：
                    随机5-10个就换行
                    弄个flag，运行一次弄个随机数，然后后面的几次就不产生随机数了
                    等到运行完指定的次数，然后再产生一个随机数
                     */
                if (ranCreateFlag) {
                    //为什么第一行是4个(SB,打印的第一行是最后一行)
                    ranCreated = scala.util.Random.nextInt(range) + star
                    ranCreateFlag = false
                }
                if (ranCreated != 0) {
                    ranCreated -= 1
                    sep + baseDataArray(ran)
                } else {
                    //需要换行
                    ranCreateFlag = true
                    "\n"
                }
            }
            // 快速添加使用列表，注意是往头部添加的。
            lst = str :: lst
        }
        lst.mkString("")
    }

    /**
      * 用于造数据的基础数据
      *
      * @param name 基础数据名
      * @return
      */
    def getBaseData(name: String): Array[String] = {
        val arr = name match {
            case "水果" =>
                Array("苹果", "梨", "橘子", "芭蕉", "草莓", "龙眼", "香蕉", "榴莲", "荔枝", "橙子", "甘蔗",
                    "枇杷", "葡萄", "芒果", "李子", "桃子", "提子", "哈密瓜", "香瓜", "木瓜", "火龙果", "猕猴桃", "雪梨",
                    "西瓜", "石榴", "香梨", "山竹", "蟠桃", "贡梨", "鸭梨", "菠萝", "柚子", "樱桃", "椰子", "无花果",
                    "山野葡萄", "桑葚", "人参果", "柿子", "杏子")
            case "四种水果" =>
                Array("苹果", "梨", "橘子", "芭蕉")
        }
        arr
    }








}
