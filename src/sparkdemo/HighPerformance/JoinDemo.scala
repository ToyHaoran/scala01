package sparkdemo.HighPerformance

import org.apache.spark.sql.expressions.UserDefinedFunction
import utils.ConnectUtil
import utils.BaseUtil._
import org.apache.spark.sql.functions._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/20
  * Time: 14:36 
  * Description:
  */
object JoinDemo {
    def main(args: Array[String]) {
        val spark = ConnectUtil.spark

        val jldxx = spark.read.parquet("hdfs://172.20.32.164:8020/YXFK/compute/HS_JLDXX")
        val ydkh = spark.read.parquet("hdfs://172.20.32.164:8020/YXFK/compute/KH_YDKH")

        /*val (joinRes,time1) = getMethodRunTime(jldxx.join(ydkh, "col1"))
        println(time1)//98.048085ms
        val (res2,time2) = getMethodRunTime(joinRes.show(20))
        println(time2)//47344.20967ms*/

        //人为制造数据倾斜
        val jldxx2 = jldxx.withColumn("col1",getCol1(col("GDDWBM")))
        val ydkh2 = ydkh.withColumn("col1",getCol1(col("GDDWBM")))

        /*//加cache内存一定要大，否则会导致内存不足Job aborted due to stage failure
        val (joinRes,time1) = getMethodRunTime(jldxx2.join(ydkh2, "col1"))
        println(time1)//72.66936ms
        val (res2,time2) = getMethodRunTime(joinRes.show(20))
        println(time2)//54024.426379ms*/

        //修正数据倾斜
        val jldxx3 = jldxx2.withColumn("col2",getCol2(col("col1")))
        jldxx3.printKeyNums("col2")
        val ydkh3 = ydkh2.withColumn("col2",getCol2(col("col1")))
        ydkh3.printKeyNums("col2")

        val (joinRes,time1) = getMethodRunTime(jldxx3.join(ydkh3, jldxx3("col2") === ydkh3("col2")))//连接有问题，怎么都成CCC了？
        println(time1)
        val (res2,time2) = getMethodRunTime(joinRes.show(20))
        println(time2)

        //joinRes.printKeyNums("col2")



        Thread.sleep(1000 * 60 * 10)
    }

    def getCol1: UserDefinedFunction = {
        udf((gddwbm:String) =>{
            val ran = scala.util.Random.nextInt(10)
            ran match {
                case _ if ran>=0 && ran <=5 => "AAA"
                case 6 => "BBB"
                case 7 => "CCC"
                case 8 => "DDD"
                case 9 => "EEE"
            }
        })
    }

    def getCol2: UserDefinedFunction = {
        udf((col1:String) =>{
            col1 match {
                case "AAA" => {
                    val ran = scala.util.Random.nextInt(5)
                    ran match {
                        case 0 => "AAA_1"
                        case 1 => "AAA_2"
                        case 2 => "AAA_3"
                        case 3 => "AAA_4"
                        case 4 => "AAA_5"
                    }
                }
                case _ => col1
            }
        })
    }

}