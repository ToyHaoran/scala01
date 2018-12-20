package utils.phoenix

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.schema.TableAlreadyExistsException
import org.apache.spark.sql.types._
import utils.{ConnectUtil, PropUtil}
import utils.BaseUtil._


//下面两个需要连接Hbase的jar包：spark-hbase-connector-2.2.0-1.1.2-3.4.6.jar
import it.nerdammer.spark.hbase.PartitionMapper._
import it.nerdammer.spark.hbase._

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StringType


/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/30
  * Time: 9:04 
  * Description: 将DF通过Phoenix存入Hbase
  */
object Save2Hbase extends App {
  /*
  spark 在211集群的启动命令：
  /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 5g --executor-memory 5g --executor-cores 3  --num-executors 5  --master yarn-cluster --class utils.phoenix.Save2Hbase --name hbase-lhr --jars /usr/local/jar/lihaoran/phoenix-4.10.0.2.6.0.3-8-client.jar,/usr/local/jar/lihaoran/spark-hbase-connector-2.2.0-1.1.2-3.4.6.jar  /usr/local/jar/lihaoran/lhrtest.jar
  所需要的jar包已经上传。
   */
  val 根据df的Schema创建Hbase表并存入数据 = 0
  if (0) {
    val spark = ConnectUtil.spark
    import spark.implicits._
    val df = spark.read.parquet(PropUtil.getValueByKey("HDFS.ROOT.162") + "/YXFK/compute/HS_DJBB")
    val tableName = "HS_DJBB"
    val keys = Array("BBBH")
    createTable(tableName, keys, df.schema)
    save2Hbase(df, tableName)
  }


  val 将HDFS上的线损计算结果parquet存到Hbase = 0
  if (1) {
    val spark = ConnectUtil.spark
    val hdfsroot = PropUtil.getValueByKey("HDFS.ROOT.211")
    val tablesAndKey = Map(("ZXSTQTJXX", Array("TQBS")), //周线损台区统计信息
      ("ZXSXLTJXX", Array("XLXDBS")), //周线损线路统计信息
      ("ZXSYHMX", Array("YHBH","JLDBH"))) //周线损用户明细

    tablesAndKey.foreach(x => {
      val table = x._1
      val keys = x._2
      println(s"${table}开始读取====↓↓↓↓================")
      val df = spark.read.parquet(s"$hdfsroot/LGYP/result/$table/2018-11-15")
      val df1 = df.na.drop(keys).dropDuplicates(keys) //删除null的和重复的主键
      df1.printSchema()
      //这个执行一次就行
      createTable(table, keys, df1.schema)
      save2Hbase(df1, table) //问题：存到Hbase中就几千条数据，是因为主键没有设置对，被覆盖了
      println(s"${table}插入完成=====↑↑↑↑================")
    })
  }


  @deprecated
  val 创建表的思路1_Shell中创建 = 0
  if (0) {
    /*
    先打印DF的schame信息，然后复制过来，Ctrl+F替换成想要的格式，数据类型也改为Phoenix中的格式。
    然后创建表，注意必须要有主键，且主键不能为null。
    可以一口气创建下面的三个表。
    但是改起来麻烦。
     */

    /*
    create table if not exists ZXSTQTJXX(
     TQBS  varchar,
     GDDWBM  varchar,
     TQBH  varchar,
     TQMC  varchar,
     XLXDBS  varchar,
     XLBH  varchar,
     XLMC  varchar,
     BQGDL double,
     BQSDL double,
     BQSSDL double,
     BQXSL  varchar,
     ZHXX  varchar,
     CBZQ  varchar,
     DQBM  varchar,
     TQKHH  varchar,
     BDZMC  varchar,
     BQZDFGS  bigint,
     ZYHS  bigint,
     BQCBS  bigint,
     ZCBS  bigint,
     XSZRR  varchar,
     FBS  bigint,
     TQRL double,
     KHZB decimal(12,6),
     KHZBXX decimal(12,6),
     RQQ  varchar,
     RQZ  varchar,
     CJSJ  varchar,
     CZSJ  varchar,
     BZ  varchar,
     XSYCBZ  varchar,
     BYLJSDL double,
     BYLJGDL double,
     BYLJXSL double,
     SQXSL  varchar
     constraint pk primary key(TQBS)
     );

    create table if not exists ZXSXLTJXX(
     RQQ varchar,
     RQZ varchar,
     XLXDBS varchar,
     XLMC varchar,
     XLBH varchar,
     GDDWBM varchar,
     BDZBS varchar,
     BDZBH varchar,
     BDZMC varchar,
     BQGDL double,
     BQSDL double,
     BQZBSDL double,
     BQSSDL double,
     BQXSL varchar,
     ZHXX varchar,
     BQZDFGS bigint,
     ZYHS bigint,
     BQCBS bigint,
     ZCBS bigint,
     XSZRR varchar,
     KHZB decimal(12,6),
     KHZBXX decimal(12,6),
     CJSJ varchar,
     CZSJ varchar,
     BZ varchar,
     DQBM varchar,
     XLKHH varchar,
     BYLJSDL double,
     BYLJGDL double,
     BYLJXSL double,
     SQXSL varchar
     constraint pk primary key(XLXDBS)
     );

    create table if not exists ZXSYHMX(
     YHBH varchar,
     JLDBH varchar,
     ZM decimal(38,18),
     GDDWBM varchar,
     DQBM varchar,
     ZHBL decimal(12,3),
     QM decimal(38,18),
     YHLBDM varchar,
     CBSJ timestamp,
     YHMC varchar,
     YDDZ varchar,
     YDLBDM varchar,
     XLXDBS varchar,
     TQBS varchar,
     CBSXH decimal(5,0),
     RL decimal(14,2),
     JLDXH decimal(5,0),
     YGZDL double,
     YGBSDL double,
     TQBH varchar,
     TQMC varchar,
     XLBH varchar,
     XLMC varchar,
     SFCB integer,
     SFZDFG integer,
     RQQ varchar,
     RQZ varchar,
     CZSJ varchar,
     CJSJ varchar,
     BZ varchar
     constraint pk primary key(YHBH)
     );
     */
  }

  val 创建表的思路2_根据dtype创建在代码中创建 = 0

  /**
    * 根据df的Schema创建表。
    * 传入表名、主键数组、Schema，创建Hbase表。
    * 如果已存在，就先删除后创建。
    * TODO 暂时不考虑列族
    */
  def createTable(tableName: String, keys: Array[String], schema: StructType): Unit = {
    //先构建SQL
    val sql = new StringBuilder()
    sql.append(s"create table $tableName(")
    for (i <- schema) {
      //组合为： 字段 VARVHAR,
      sql.append(i.name).append(" ").append(getHbaseType(i.dataType)).append(",")
    }
    sql.deleteCharAt(sql.length - 1) //删除最后一个逗号
    sql.append(s" constraint pk primary key(${keys.mkString(",")}))") //增加主键

    //然后执行创建语句
    val connection = PhoenixUtil.getConnection
    try {
      PhoenixUtil.execute(sql.toString(), connection)
    } catch {
      case e: TableAlreadyExistsException =>
        //如果表已经存在，就先删除再创建
        PhoenixUtil.execute(s"drop table $tableName", connection)
        PhoenixUtil.execute(sql.toString(), connection)
    }
  }

  /**
    * 将parquet中的类型转为Hbase对应的类型。
    * Phoenix数据类型: https://blog.csdn.net/jiangshouzhuang/article/details/52400722
    *
    * @param flag parquet中对应的类型
    * @return Hbase中的类型，字符串
    */
  def getHbaseType(flag: Any): String = {
    flag match {
      case a: IntegerType => "INTEGER"
      case a: LongType => "BIGINT"
      case a: ByteType => "TINYINT"
      case a: ShortType => "SMALLINT"
      case a: FloatType => "FLOAT"
      case a: DoubleType => "DOUBLE"
      case a: DecimalType => a.typeName //decima(6,2)之类的
      case a: BooleanType => "BOOLEAN"
      case a: TimestampType => "TIMESTAMP"
      case a: DateType => "DATE"
      case a: StringType => "VARCHAR"
      case _ => "VARCHAR"
    }
  }

  /**
    * 将DF存到Hbase，前提是与表结构一致
    */
  def save2Hbase(df: DataFrame, table: String): Unit = {
    df.write.format("org.apache.phoenix.spark")
        .mode(SaveMode.Overwrite)
        .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
        .option("zkUrl", PropUtil.getValueByKey("PHOENIX.URL"))
        .option("table", table)
        .save()
  }
}