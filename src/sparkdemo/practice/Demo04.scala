package sparkdemo.practice

//有sexMap存放性别编码及对应的文字内容，如下所示：
//val sexMap:Map[String,String] = Map(("1"->"男"),("0"->"女"))
//现有stuinfo.txt，内存有四位学生信息，每位学生有四个属性，分编为编号，名称，性别，地区，
// 对应的Schema信息为code(String),name(Stgring),sex(String),area(String)，
// 读取stuinfo.txt中学生信息，按照给定的Schema转换成DataFrame，
// 然后使用UDF和广播变量相关知识在不改变Schema情况下将sex字段中的数字转换成sexMap中对应的文字。

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

object Demo04 extends App{
    val sc = new SparkContext(new SparkConf().setAppName("Exam03").setMaster("local[2]"))
    val spark = SparkSession.builder().appName("Exam04").config("spark.some.config.option", "some-value").getOrCreate()

    //答案
    import spark.implicits._
    val sexMap: Map[String, String] = Map(("1" -> "男"), ("0" -> "女"))
    val bro = spark.sparkContext.broadcast(sexMap)
    val df = spark.read.textFile("F:\\桌面\\API\\Scala\\SparkDemo1\\source\\stuinfo.txt").map(line => {
        val fields = line.split(",")
        (fields(0), fields(1), fields(2), fields(3))//注意DS中不能返回Row类型，否则报错
    }).toDF("code","name","sex","area")

    //如果udf和col是红色的，说明没有这个函数，其实需要导包
    import org.apache.spark.sql.functions._
    val df2Map = udf((sex: String) => {
        val map = bro.value
        val gender = map.getOrElse(sex, "null")
        gender
    })

    val newDF = df.select("code", "name", "sex", "area")//感觉这里可以直接用expr函数。
        .withColumn("gender",df2Map(col("sex")))
        .drop("sex")
        .withColumnRenamed("gender", "sex")
        .select("code", "name", "sex", "area")

    newDF.show()
    spark.stop()

}