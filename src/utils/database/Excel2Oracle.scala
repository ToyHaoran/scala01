package utils.database

import java.sql.{SQLSyntaxErrorException, Types}

import utils.BaseUtil._
import utils.ConnectUtil
import org.apache.spark.sql.expressions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/12/12
  * Time: 8:34 
  * Description:
  */
object Excel2Oracle extends App {

  /*
    1、集群启动命令:
      /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 5g --executor-memory 5g --executor-cores 4  --num-executors 5  --class utils.database.Excel2Oracle  --name DBtest-lhr --driver-class-path /usr/local/jar/lihaoran/ojdbc7.jar  --jars /usr/local/jar/lihaoran/ojdbc7.jar /usr/local/jar/lihaoran/lhrtest.jar
    2、报错：
      1、如果在yarn上报错：找不到文件；而在shell中可以运行。
        解决：是因为你将文件上传到了一个节点，而其他节点上没有这个数据，
          方式1、放到HDFS上，可以直接运行yarn
          方式2、调整启动模式为client，去掉：--master yarn-cluster
   */
  val 读取一个Excel并存到数据库 = 0
  if (0) {
    val spark = ConnectUtil.spark
    //先读取一个文件试试
    var excel = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("file:/usr/local/jar/lihaoran/cb2.csv")
    //建表一般前端会给表结构，需要做的就是将自动推断的错误的类型转换一下。
    excel = excel
        .withColumn("GDDWBM", col("GDDWBM").cast(StringType))
        .withColumn("DQBM", col("DQBM").cast("string"))
    try {
      createTable("CBXX", excel.schema)
      println("创建表成功========")
    } catch {
      case e: SQLSyntaxErrorException =>
        println("创建表失败，可能表已存在，直接插入数据========")
        println(e.getMessage)
    }
    JdbcUtil.save("dfjs", "CBXX", excel)
    println("插入表成功=============")
  }


  /**
    * 传入表名、主键数组、Schema，创建Hbase表。
    * 如果已存在，就先删除后创建。
    */
  def createTable(tableName: String, schema: StructType, keys: Array[String] = Array()): Unit = {
    //先构建SQL
    val sql = new StringBuilder()
    sql.append(s"create table $tableName(")
    for (i <- schema) {
      //组合为： 字段 VARVHAR,
      sql.append(i.name).append(" ").append(getOracleType(i.dataType)).append(",")
    }
    if (keys.isEmpty) {
      //没有主键
      sql.deleteCharAt(sql.length - 1) //删除最后一个逗号
      sql.append(")")
    } else {
      //有主键
      //和Hbase的区别是最后一个逗号要加着
      sql.append(s" constraint pk primary key(${keys.mkString(",")}))")
    }
    JdbcUtil.execute("dfjs", sql.toString())
  }

  /**
    * 对应的数据库类型：https://blog.csdn.net/s592652578/article/details/78518148
    * https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
    */
  def getOracleType(flag: Any): String = {
    flag match {
      case a: IntegerType => "INTEGER(10)"
      case a: LongType => "NUMBER(19)"
      case a: ShortType => "NUMBER(5)"
      case a: FloatType => "NUMBER(19,4)"
      case a: DoubleType => "NUMBER(19,4)" //double必须制定位数
      case a: DecimalType => a.typeName //decima(6,2)之类的
      case a: BooleanType => "NUMBER(1)"
      case a: TimestampType => "DATE"
      case a: DateType => "DATE"
      //改为varchar2类型，节省空间
      case a: StringType => "VARCHAR2(40)"
      case _ => "VARCHAR(40)"
    }
  }

  val 手工创建表 = 0
  /*
    create table cbxx(
     YHBH   VARCHAR2(30),
     JLDBH   VARCHAR2(30),
     ZCBH   VARCHAR2(30),
     CBRQ timestamp,
     DFNY NUMBER,
     ZDXL VARCHAR2(30),
     FYGZDXL VARCHAR2(30),
     ZXYGZ NUMBER,
     ZXYGF NUMBER,
     ZXYGP NUMBER,
     ZXYGG NUMBER,
     ZXYGJ NUMBER,
     ZXWGZ NUMBER,
     ZXWGF NUMBER,
     ZXWGP NUMBER,
     ZXWGG NUMBER,
     ZXWGJ NUMBER,
     ZXYGA VARCHAR2(30),
     ZXYGB VARCHAR2(30),
     ZXYGC VARCHAR2(30),
     FXYGZ NUMBER,
     FXYGF NUMBER,
     FXYGP NUMBER,
     FXYGG NUMBER,
     FXYGJ NUMBER,
     FXWGZ NUMBER,
     FXWGF NUMBER,
     FXWGP NUMBER,
     FXWGG NUMBER,
     FXWGJ NUMBER,
     GDDWBM VARCHAR2(30),
     DQBM VARCHAR2(30),
     CJSJ timestamp,
     YXGXBZ VARCHAR2(30),
     YXGXSJ VARCHAR2(30),
     YXGXYCSM VARCHAR2(30),
     constraint pk primary key(GDDWBM,YHBH)
    );
   */

}