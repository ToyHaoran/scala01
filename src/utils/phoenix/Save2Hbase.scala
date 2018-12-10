package utils.phoenix

import java.util.UUID

import org.apache.hadoop.conf.Configuration
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

    val 将HDFS上的parquet存到Hbase = 0
    if (1) {
        val spark = ConnectUtil.spark
        val hdfsroot = PropUtil.getValueByKey("HDFS.ROOT.211")
        val tqdf = spark.read.parquet(hdfsroot + "/LGYP/result/ZXSTQTJXX/2018-10-15") //周线损台区统计信息
        val xldf = spark.read.parquet(hdfsroot + "/LGYP/result/ZXSXLTJXX/2018-10-15") //周线损线路统计信息
        val yhmxdf = spark.read.parquet(hdfsroot + "/LGYP/result/ZXSYHMX/2018-10-15") //周线损用户明细
        /*println(tqdf.count()) //1119995
        println(xldf.count()) //811332075
        println(yhmxdf.count()) //312160*/

        val getFormat = udf((rq:String)=>{
            rq.replace("-","")
        })

        /*val df1 = tqdf.na.drop(Seq("TQBS")).dropDuplicates("TQBS")
            .withColumn("RQQ",getFormat(col("RQQ")))
            .withColumn("RQZ",getFormat(col("RQZ")))
        df1.printSchema()
        println(df1.count())
        val df2 = xldf.na.drop(Seq("XLXDBS")).dropDuplicates("XLXDBS")
            .withColumn("RQQ",getFormat(col("RQQ")))
            .withColumn("RQZ",getFormat(col("RQZ")))
        df2.printSchema()
        println(df2.count())*/
        val df3 = yhmxdf.na.drop(Seq("YHBH")).dropDuplicates("YHBH")
            .withColumn("RQQ",getFormat(col("RQQ")))
            .withColumn("RQZ",getFormat(col("RQZ")))
        df3.printSchema()
        println(df3.count())

        //然后将记录存入上面创建的表
        /*
        问题：存到Hbase中就几千条数据，是因为主键没有设置对。
         */
        /*save2Hbase(df1, "ZXSTQTJXX")
        println("df1   ok")
        save2Hbase(df2, "ZXSXLTJXX")
        println("df2   ok")*/
        save2Hbase(df3, "ZXSYHMX")
        println("df3   ok")

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


    val 创建表的思路1_Shell中创建 = 0
    if (0) {
        /*
        先打印DF的schame信息，然后复制过来，Ctrl+F替换成想要的格式，数据类型也改为Phoenix中的格式。
        然后创建表，注意必须要有主键，且主键不能为null。
        可以一口气创建下面的三个表。
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

    val X创建表的思路2_根据dtype创建在代码中创建 = 0
    //拼接一个创建表的字符串，放弃，太麻烦，还不灵活。以后实际需要的时候再做。
    if (0) {
        //先通过一条记录创建一个表。
        val connection = PhoenixUtil.getConnection
        PhoenixUtil.execute("创建表的字符串", connection)
    }

    val Phoenix数据类型 = 0
    // Phoenix数据类型: https://blog.csdn.net/jiangshouzhuang/article/details/52400722

}