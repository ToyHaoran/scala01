package sparkdemo

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import utils.BaseUtil._
import utils.database.JdbcUtil
import utils.{ConnectUtil, PropUtil}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/20
  * Time: 14:36 
  * Description:
  */
object JoinDemo extends App {

  val 如何在1亿条数据中更新数据 = 0
  if (0) {
    val spark = ConnectUtil.spark
    //主键
    val keys = Array("YHBH")
    //原始DF
    val source = spark.read.parquet(PropUtil.getValueByKey("HDFS.ROOT.162") + "/YXFK/compute/KH_YDKH_TEMP")
    //增量DF
    val sql = s"(SELECT * FROM KH_YDKH_TEMP WHERE CZSJ > to_date('2018-12-04 18:31:30','yyyy-mm-dd hh24:mi:ss') AND CZSJ <= to_date('2018-12-04 23:59:59','yyyy-mm-dd hh24:mi:ss'))"
    val update = JdbcUtil.loadByColumn("dfjs", sql, "GDDWBM")
    //Join结果
    val unionDf = updateBigDf(source, update, keys)
  }

  val 将连接条件简化 = 0
  if (0) {
    val df1 = spark.createDataFrame(List(("a", "1", "2"), ("b", "2", "3"), ("c", "3", "4"))).toDF("id", "v1", "v2")
    val df2 = spark.createDataFrame(List(("d", "1", "2"), ("e", "5", "6"), ("c", "3", "4"))).toDF("id", "v1", "v2")
    val keysOpt = Array("id")
    val conditions: Array[Column] = keysOpt.map(key => df1(key) === df2(key))
    val head = conditions.head
    val tail = conditions.tail
    val res = tail.foldLeft(head)(_ && _)
    val ss = df1.join(df2, res, "fullouter")
    ss.show()
    ss.select(df1.columns.map(c => df2(c)): _*).show()
    ss.select(df1.columns.map(c => df1(c)): _*).show()
  }

  /**
    * 根据主键更新大表。
    *
    * @param sourceDf 原始的大表DF，上亿数据
    * @param updateDf 需要更新的数据,上百万数据
    * @param keys     主键列
    * @return 更新后的DF
    */
  def updateBigDf(sourceDf: DataFrame, updateDf: DataFrame, keys: Array[String]): DataFrame = {
    //增加判断是否更新的标志
    val df1 = sourceDf.withColumn("FLAG", lit("1"))
    val df2 = updateDf.withColumn("FLAG", lit("1"))
    //控制字段选择的顺序
    val columnName = sourceDf.columns
    //将df1和df2进行fullouter join，df1是大表。
    //方案1：已经废弃：val df3 = getCondition(df1, df2, keys)
    //方案2：
    val conditions: Array[Column] = keys.map(key => df1(key) === df2(key))
    val head = conditions.head
    val tail = conditions.tail
    val df3 = df1.join(df2, tail.foldLeft(head)(_ && _), "fullouter")
    /*
    逻辑：
        进行outjoin后判断Flag标记：
        1   ，null   表示不需要更新
        1   ，1      表示需要更新
        nul ，1      表示增量数据
     */
    //如果是需要更新的数据就选择 增量DF 的字段
    val df4 = df3.filter(df2("FLAG") === "1").select(columnName.map(c => df2(c)): _*)
    //如果是不需要更新的数据就选择 原始DF 的字段
    val df5 = df3.filter(df2("FLAG").isNull).select(columnName.map(c => df1(c)): _*)
    df4.union(df5).drop("FLAG")
  }

  /**
    * 将DF1和DF2根据主键个数进行full outer join
    *
    * @param df1  大表
    * @param df2  小表(在右边会自动广播)
    * @param keys 主键列
    * @return join后的DF
    */
  @deprecated("被foldLeft函数取代")
  def getCondition(df1: DataFrame, df2: DataFrame, keys: Array[String]): DataFrame = {
    /*
      这里有个Bug：
      1、如果用Seq(String)作为条件，select的时候就会导致df2,df1的主键消失
          这种情况下，无论是select(df1("key1")),还是df2("key1"),都会报错：key1不存在：
          coalesce(key1#204, key1#218) AS key1#458, key2#205, key3#206, key2#219, key4#220]
          上面的报错信息可以看到原来的键被合并了。
          必须select(col("key1"),df1("key2"),df2("key3"))，这个select语句不好拼接。
              可以在map里面加条件判断是不是主键，然后进行拼接。
              主键多了判断也多。。
              参考一下张春振的那个getMap,有点用。
      2、如果用df1("YHBH") === df2("YHBH")，条件还不能统一制定。
          可以判断主键有几个，然后按主键个数选择连接条件
      3、如果注册成临时表：
          判断条件也不好写，而且select a.* 字段也不一定能对齐。
      4、用RDD处理
          性能较低，上亿数据处理不了。
      综上：选择第二种方式。
       */
    val keysSize = keys.length
    val joinType = "fullouter"
    keysSize match {
      case 1 =>
        val key1 = keys(0)
        df1.join(df2, df1(key1) === df2(key1), joinType)
      case 2 =>
        val (key1, key2) = (keys(0), keys(1))
        df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2), joinType)
      case 3 =>
        val (key1, key2, key3) = (keys(0), keys(1), keys(2))
        df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2) && df1(key3) === df2(key3), joinType)
      case 4 =>
        val (key1, key2, key3, key4) = (keys(0), keys(1), keys(2), keys(3))
        df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2) && df1(key3) === df2(key3) &&
            df1(key4) === df2(key4), joinType)
      case 7 =>
        val (key1, key2, key3, key4, key5, key6, key7) = (keys(0), keys(1), keys(2), keys(3),
            keys(4), keys(5), keys(6))
        df1.join(df2, df1(key1) === df2(key1) && df1(key2) === df2(key2) && df1(key3) === df2(key3) &&
            df1(key4) === df2(key4) && df1(key5) === df2(key5) && df1(key6) === df2(key6)
            && df1(key7) === df2(key7), joinType)
    }
  }


}