package sparkdemo.practice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


//部分数据
//TableA
//0,郎爽
//1,刁悦梦
//2,戴崈
//3,解勒劼
//4,伏擎
//5,林芸

//TableB
//0,94
//0,97
//0,48
//1,45
//1,97
//1,96
//1,92
//1,34
//2,49
//2,85

//对于某学校的一次期末考试成绩，表A(ID long(主键), NAME String(姓名))为学生信息表，表B(ID long(外键), score int(成绩0-100))为学生成绩表，
//要求：
//1、计算每个学生的总成绩TOTALSCORE（所有成绩相加，比如学生小明的成绩为 70,65,75,80则总成绩为70+65+75+80=290）。
//2、计算每个学生的平均成绩AVGSCORE，结果保留2位小数（总成绩除以数据条数，比如小明的平均成绩为290/4=72.5）。
//3、根据平均成绩判断该学生是否及格PASS(AVGSCORE>=60)，如果及格标记为1，否则为0(小明成绩为72.5>60，所以小明及格)。
//3、计算结果格式为(ID long(主键), NAME String(姓名), TOTALSCORE long(总成绩), AVGSCOR DECIMAL(2,2)(Double也可)(平均成绩), PASS int(是否及格))。
//4、计算结果按照TOTALSCORE降序排序。
//5、计算原始数据从txt加载(字符串分隔符为",")，结果保存为parquet。


object Demo03 {
    def main(args: Array[String]) {

        //    val conf       = new SparkConf().setAppName("Scala UDF Example").setMaster("local")
        //    val spark      = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("demo")
        val sc: SparkContext = new SparkContext(sparkConf)
        val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

        //第三次使用的方法：使用DF做,目前最完美的方案
        import spark.implicits._
        //注意虽然方法相同，但是一个是RDD的方法，一个是DataSet的方法。
        val dfa = sc.textFile("src/sparkdemo/testfile/TableA.txt")
            .map(_.split(","))//返回值是value
            .map(x=>(x(0).toLong,x(1)))//返回值是两列
            .toDF("ID","NAME")
        val dfb = spark.read.textFile("src/sparkdemo/testfile/TableB.txt")
            .map(_.split(","))
            .map(x=>(x(0).toLong,x(1).toLong))//返回值是两列
            .toDF("ID","SCORE")

        val dfc = dfb.groupByKey(_.getLong(0)).mapGroups((id,groups)=>{
            //求和
            var sum = 0L//注意类型
            //次数
            var num = 0L

            for (i<-groups){
                sum += i.getLong(1)
                num += 1
            }
            //平均分(注意转化为Double,否则小数会被省去)
            val avg = sum.toDouble/num.toDouble
            //是否通过
            val pass = if(avg > 60) 1 else 0

            (id,sum,avg.toDouble.formatted("%.2f"),pass)
        }).toDF("ID","TOTALSCORE","AVGSCORE","PASS")

        val res = dfa.join(dfc,"ID").orderBy($"TOTALSCORE".desc)
        res.repartition(5).write.mode(SaveMode.Overwrite).parquet("src/sparkdemo/testfile/Demo03ParquetResult")


        //第二次使用的方法：使用RDD进行处理，最后连接的时候居然还是用DF做的，也很垃圾。
        //    val idAndName = spark.sparkContext.textFile("F:\\桌面\\API\\Scala\\SparkDemo1\\src\\source\\practice\\TableA.txt")
        //    val idAndScores =spark.sparkContext.textFile("F:\\桌面\\API\\Scala\\SparkDemo1\\src\\source\\practice\\TableB.txt")
        //
        //    val nameRDD = idAndName.map(line => line.split(",")).map(x => (x(0).toLong,x(1))).map(x=>Row(x._1,x._2))
        //    val scoreRDD = idAndScores.map(line => line.split(",")).map(x => (x(0).toLong,x(1).toLong))
        //
        //      //将score中的数据求和，次数
        //      def func(a:(Long,Int),b:(Long,Int)):(Long,Int) ={
        //          val sum = a._1+b._1  //求和
        //          val num = a._2+b._2   //次数
        //          (sum,num)
        //      }
        //
        //      //返回值为id，sum，平均分，是否通过，并按照sum排序
        //      val resultOfScore = scoreRDD.map(x =>(x._1,(x._2,1))).reduceByKey(func _).map(x=>{
        //          val avg = (x._2._1/x._2._2)
        //          val pass = if(avg > 60) 1 else 0
        //          (x._1,x._2._1,avg.toDouble.formatted("%.2f"),pass) //次数不需要返回
        //      }).sortBy(x=>x._2,false).map(x=>Row(x._1.toLong,x._2.toLong,x._3.toDouble,x._4.toInt))
        //      //res62: Array[(Long, Long, Int, String, Int)] = Array((5972,554,6,92.00,1), (1062,548,6,91.00,1),....
        //
        //      val schema1 = StructType( StructField("ID",LongType,false)::StructField("NAME",StringType,true)::Nil)
        //      val schema2 = StructType( StructField("ID",LongType,false)::StructField("TOTALSCORE",LongType,true)::StructField("AVGSCORE",DoubleType,true)::StructField("PASS",IntegerType,true)::Nil)
        //
        //      val nameDF = spark.createDataFrame(nameRDD,schema1)
        //      val scoreDF = spark.createDataFrame(resultOfScore,schema2)
        //
        //      val res = scoreDF.join(nameDF,"ID").orderBy(desc("TOTALSCORE"))
        //
        //      res.show()
        //      val output = res.write.parquet("F:\\桌面\\API\\Scala\\SparkDemo1\\src\\source\\practice\\test2.parquet")




        //第一次做的方法：比较垃圾
        //直接用一句Sql语句解决问题。
        //    val nameRDD = idAndName.map(line => line.split(",")).map(x => Row(x(0).toLong,x(1)))
        //    val scoreRDD = idAndScores.map(line => line.split(",")).map(x => Row(x(0).toLong,x(1).toLong))
        //
        //    //创建schema
        //    val schema1 = StructType( StructField("id",LongType,false)::StructField("name",StringType,true)::Nil)
        //    val schema2 = StructType( StructField("id",LongType,false)::StructField("score",LongType,true)::Nil)
        //
        //    val nameDF = spark.createDataFrame(nameRDD,schema1)
        //    val scoreDF = spark.createDataFrame(scoreRDD,schema2)
        //
        //    //创建临时表
        //    nameDF.createOrReplaceTempView("name")
        //    scoreDF.createOrReplaceTempView("score")
        //
        //    val res = spark.sql("select a.id,sum(b.score) as TOTALSCORE,avg(b.score) as AVGSCORE, (case when avg(b.score)>=60 then 1 else 0 end) as PASS  " +
        //      "from name a,score b where a.id = b.id group by a.id order by TOTALSCORE desc" )
        //
        //    res.persist()
        ////    res.createOrReplaceTempView("res")
        //    res.show()
        //    val output = res.write.parquet("F:\\桌面\\API\\Scala\\SparkDemo1\\src\\source\\practice\\test2.parquet")

    }
}

