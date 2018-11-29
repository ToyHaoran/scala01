package sparkdemo.practice

import java.util.UUID

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import utils.{ConnectUtil, DataCreateUtil}
import utils.BaseUtil.int2boolen

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/1
  * Time: 16:00 
  * Description:
  * 给一篇英文文档，实现WordCount，最后的结果为DataFrame，Schema信息为PKID（String），WORD（String），FREQ（LONG）
  * 其中PK为主键使用UUID生成，WORD是单词，FREQ为词频，按照词频有大到小的顺序进行排序并输出所有结果。
  */
object Demo01 extends App {

    val spark = ConnectUtil.spark
    val sc = ConnectUtil.sc

    import spark.implicits._

    if (0) {
        //方法1:推荐
        //val words1 = sc.textFile("src/sparkdemo/testfile/hello.txt")
        //val words1 = sc.textFile("file:/usr/local/jar/lihaoran/hello.txt")
        val words1 = sc.parallelize(Seq(DataCreateUtil.textCreate(400000, " ")))
            .flatMap(line => line.split("\\s+"))
            .map(x => (x, 1))
            .reduceByKey(_ + _)
            .sortBy(_._2, false)
            .map(x => (UUID.randomUUID().toString, x._1.toString, x._2.toLong))
            .toDF("PKID", "WORD", "FREQ")
            .show()

    }

    if (0) {
        //方法2 费劲
        val words = sc.textFile("file:///F:/桌面/API/Scala/SparkDemo1/src/source/hello.txt")
            .flatMap(line => line.split("\\s+"))
            .map(x => (x, 1))
            .reduceByKey(_ + _)
            .sortBy(_._2, false)
            .map(x => Row(UUID.randomUUID().toString, x._1.toString, x._2.toLong))
        val struct = StructType(StructField("PKID", StringType, false) ::
            StructField("WORD", StringType, true) ::
            StructField("FREQ", LongType, true) :: Nil)

        val res = spark.createDataFrame(words, struct) //费劲
        res.show()
    }
}