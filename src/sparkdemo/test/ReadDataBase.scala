package sparkdemo.test

import java.io.IOException

import utils.BaseUtil._
import utils.database.{JdbcUtil, PredicatesUtil}
import utils.{ConnectUtil, HDFSUtil, PropUtil}
import utils.UdfUtil._
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import java.util.concurrent.ConcurrentSkipListSet

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/22
  * Time: 14:01 
  * Description:
  */
object ReadDataBase extends App {
  val 教训 = 0
  if (0) {
    //修改完数据库一定要点击左上角的提交。卧槽，找Bug卡了我3个小时。。。老长时间不玩数据库生疏了。1H以上就找别人帮忙。
  }

  /*
   读取正确的parquet，然后读取错误的parquet，然后测试错误的parquet是否是重复数据，检查是否有漏掉的数据。
   */
  val 测试正确数据和错误数据 = 0
  if (1) {
    //todo  能够对比Oracle和Parquet文件
    val correct = JdbcUtil.load("dfjs", "HS_DJBB")
   /* val wrong = ConnectUtil.spark.read.format("jdbc")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("url", "jdbc:oracle:thin:@172.20.32.210:1521/hycx")
        .option("user", "RT_DFJS")
        .option("password", "ffffff")
        .option("dbtable", "(SELECT t.*,ROWNUM rownum_rn FROM HS_DJBB t) b")
        .option("fetchsize", "5000")
        .option("partitionColumn", "rownum_rn")
        .option("lowerBound", "0")
        .option("upperBound", "1500")
        .option("numPartitions", "10")
        .load()
        .drop("rownum_rn")*/
    val wrong = JdbcUtil.loadByRownum("dfjs", "HS_DJBB", 10)

    correct.cache()
    wrong.cache()
    println("数量对比================")
    println(s"correct: ${correct.count()}")
    println(s"wrong: ${wrong.count()}")

    //todo 主键数量对比，能大大加快对比速度，问题是主键是怎么获取？
    correct.printPartItemNum()
    wrong.printPartItemNum()

    println("取差集，按理说应该等于0============")
    val df1 = correct.except(wrong)
    val df2 = wrong.except(correct)
    println(s"correct有,但是wrong没有： ${df1.count()}")
    df1.show()
    println(s"wrong有,但是correct没有： ${df2.count()}")
    df2.show()


  }

  /*
  集群启动命令：
  /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 15g --executor-memory 15g --executor-cores 4  --num-executors 25  --master yarn-cluster --class sparkdemo.test.ReadDataBase --name dbtest-lhr --driver-class-path /usr/local/jar/lihaoran/ojdbc7.jar  --jars /usr/local/jar/lihaoran/ojdbc7.jar /usr/local/jar/lihaoran/lhrtest.jar

  步骤：
      1、删除原来的表：drop table KH_YDKH_TEMP
      2、创建新的表：
          注意：要在表中数据全部插入结束后再创建索引，否则插一条更新一次索引会卡死的，速度相差上百倍
          1、右键表——查看——右下角查看SQL——只复制创建table的语句
          2、到新的数据库或新的SQL窗口——粘贴——改表名——执行
      3、从Parquet中读取数据
      4、追加到数据库中5次，总数据量1亿
          建立索引：create index IDX_SB_YXDNB_TEMP_GDDWBM on SB_YXDNB_TEMP (GDDWBM);
          查询的时候，不建立索引会非常的慢。

  报错：
      1、ORA-01653: unable to extend table RT_DFJS.KH_JLD_TEMP by 8192 in tablespace HYBDS
          参考：表空间不足   https://my.oschina.net/u/2381678/blog/654554
          解决：删除表    https://blog.csdn.net/qq_40087415/article/details/78812019
      2、产出表删不掉：ORA-00054: resource busy and acquire with NOWAIT specified
          是原来的SQL语句产生了锁(索引的问题，导致还没执行完)
          select username,sid,machine from v$locked_object,v$session where v$locked_object.session_id=v$session.sid;
          select sql_text from v$session,v$sqltext_with_newlines where decode(v$session.sql_hash_value,0,prev_hash_value,sql_hash_value)=v$sqltext_with_newlines.hash_value and v$session.sid=&sid order by piece;
          alter system kill session '2100'; --权限不足 删除失败
          解决：找管理员。。。
   */
  val 几个大表创建1亿条数据从HDFS存入Oracle = 0
  if (0) {
    val spark = ConnectUtil.spark
    //需要修改以下内容：
    val srcTable = "ZW_SSDFJL" //原来的表
    val keys = "YSZWLSH,JLDBH,SSZWLSH" //主键
    val num = 6 //插入多少次
    val db = "dfjs" //需要插入的数据库

    val desTable = srcTable + "_TEMP" //需要插入的表
    val keysArr = keys.split(",") //切割主键
    val keyslen = keysArr.length //主键个数
    //从HDFS读取
    val df = spark.read.parquet(PropUtil.getValueByKey("HDFS.ROOT.162") + "/YXFK/compute/" + srcTable)
    df.cache()
    println(srcTable + "---记录数------" + df.count())

    for (i <- 1 to num) {
      println(s"第${i}遍==============")
      //对主键切割前缀，然后加后缀，保证主键唯一性
      val getUdf = udf((x: String) => x.substring(1) + i.toString)
      val data = keyslen match {
        case 1 =>
          //一个主键的情况
          val key1 = keysArr(0)
          df.withColumn(key1, getUdf(col(key1))).na.drop(Seq(key1))
        case 3 =>
          //三个主键的情况
          val (key1, key2, key3) = (keysArr(0), keysArr(1), keysArr(2))
          df.withColumn(key1, getUdf(col(key1)))
              .withColumn(key2, getUdf(col(key2)))
              .withColumn(key3, getUdf(col(key3))).na.drop(Seq(key1, key2, key3))
      }
      JdbcUtil.save(db, desTable, data, SaveMode.Append)
    }
    println("全部插入完成==============")
  }

  /*
   比如说压力测试的时候不能在YXFK运行。需要先创建多个空表。
   创建空表的方式：
    1、使用navicat传输数据，不好用。。
    2、推荐：使用PL/SQL导出表结构，然后切换到另一个数据库进行导入。（直接导出全部数据也可以）
      导出：工具——导出表——SQL插入——只选择第二个创建表——Where子句中写ROWNUM<1——选择你需要导出的表——输出文件——导出
      导入：工具——导入表——SQL插入——导入文件——导入
      然后使用下面这个程序导入记录，适合大量数据。
    3、手动复制创建表的语句，然后使用下面这个程序导入
    4、直接使用PL/SQL导出（大量数据，必须在服务端运行）：
        数据泵(expdp/impdp)可以通过使用并行，从而在效率上要比exp/imp 要高。
        在导出的时候Oracle导出：导出可执行文件，然后选择oracle/bin目录下的数据泵expdp.exe
        导入时选择：impdb.exe。
        关于速度：Oracle导出(dmp、批量、速度快、无法修改) >> SQL插入(sql、一条一条、速度慢、可修改)
   */
  val 将Oracle表复制到测试的Oracle = 0
  if (0) {
    val spark = ConnectUtil.spark
    val tables = "HS_DJBB"
    tables.split(",").foreach(table => {
      //todo 加分区列分区读取
      val df = JdbcUtil.load("dfjs", table)
      df.show()
      JdbcUtil.save("dfjs", table + "_2", df)
    })
  }

  /*
  重新初始化需要的表
   */
  val 初始化表并备份原来的数据 = 0
  if (0) {
    //ZW_FK_YHDA,ZW_SSDFJL,KH_YDKH,FW_WQXX,FW_KFGDXX,FW_GDYHGL  OK
    //大表：FW_YKJLDLSXX,KH_JLD,KH_JSH,LC_HBXXJL  OK
    //小表：DW_YXBYQ,FW_YKYXBYQLSXX,HS_CQDLMX,HS_JTBS,HS_MFDYH,   OK
    // LC_CBQDXX,XT_RY,ZW_FK_HCYCJL,ZW_FK_TFDGZD,ZW_FK_YHZZXX,ZW_FK_YJGZD   OK
    val tables = "FW_YKJLDLSXX,KH_JLD,KH_JSH,LC_HBXXJL"
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.162")
    val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
    hdfsUtil.setUser("root")
    tables.split(",").par.foreach(table => {
      val df = JdbcUtil.loadTable("gzdb", table, "GDDWBM")
      val path = hdfsRoot + "/YXFK/compute/" + table
      val path_bak = path + "_BAK"
      try {
        hdfsUtil.rename(path, path_bak)
        df.write.parquet(path)
        println(s"$table 写入Parquet成功==========")
      } catch {
        case e: IOException =>
          println(table + "重命名失败=========")
          e.printStackTrace()
        case e: Exception =>
          println(table + "有问题，请检查路径==========")
          e.printStackTrace()
      }
    })
  }

  val 还原原来的数据 = 0
  if (0) {
    val tables = "ZW_SSDFJL,KH_YDKH,FW_WQXX,FW_KFGDXX,FW_GDYHGL"
    val hdfsRoot = PropUtil.getValueByKey("HDFS.ROOT.162")
    val hdfsUtil = HDFSUtil.getInstance(hdfsRoot)
    hdfsUtil.setUser("root")
    tables.split(",").foreach(table => {
      val path = hdfsRoot + "/YXFK/compute/" + table
      val path_bak = path + "_BAK"
      if (hdfsUtil.exists(path) && hdfsUtil.exists(path_bak)) {
        hdfsUtil.delete(path)
        hdfsUtil.rename(path_bak, path)
      } else {
        println(table + "路径不正确，请检查======")
      }
    })
  }

  /*
  关于读取数据库的一些经验：
  1、spark.read.jdbc读取的时候一定要分区读取
    a、通过某个带有索引的字段分区读取，比如说GDDWBM。
    b、使用某个数值列分区读取，但是需要控制数据起始和间隔。
    c、使用随机数读取，这种情况下，数据是随机分到每个分区的。计算的时候最好重分区一下。
  2、数据库相关的问题：
    a、没有索引的情况下插入快，但是读取慢。
    b、有索引的情况下读取快，但是插入巨慢。
    c、正在研究主键索引对插入的影响，听刘波说是数字类型自增列主键比较快，而字符串主键插入巨慢。
    d、此外还有批量插入的影响，待研究。
  3、并行读取:
    a、一定要并行，虽然同时运行的就66个task，但是如果有空余的task可以用来运行那种无法并行的读取时间还长的task
    b、那些无法并行运行，时间还长的任务，最好先运行，避免到最后就那几个单线程吭哧吭哧的运行。其他的线程还没事干。
  4、缓存问题：
    a、如果要写入hdfs两次，必须加缓存，否则会从数据库读取两次，双倍时间。
    b、没用的缓存，如果能手动unpersist，就直接处理，不要等到垃圾回收。


  问题：
      1、查看数据库连接数：https://blog.csdn.net/zmx729618/article/details/54018629
   */
  val 读取Oracle数据库速度测试 = 0
  if (0) {
    //val tables = "SB_YXDNB,KH_JLD,ZW_SSDFJL,KH_JSH,KH_YDKH" //600万以上的表 LC_YXDNBSS（没有GDDWBM）
    val db = "yxfk"
    val tables = "SB_YXDNB,KH_JLD,ZW_SSDFJL,KH_JSH,KH_YDKH,LC_YXDNBSS"
    val parTable = tables.split(",").par
    println(s"一共${parTable.size}个表")

    parTable.foreach(table => {
      try {
        var df: DataFrame = null
        val (count, time1) = getMethodRunTime({
          df = JdbcUtil.loadTable(db, table)
          df.count()
        })
        println(s"${table}数量：$count,读取时间：$time1 =========")
        //如果要写入parquet，一定要先加缓存，否则会读取两次
        val (res2, time2) = getMethodRunTime({
          (0 to 1).par.foreach {
            case 0 =>
              df.write.mode(SaveMode.Overwrite).parquet(s"${PropUtil.getValueByKey("HDFS.ROOT.162")}/lihaoran/YXFK/compute/$table")
            case 1 =>
              df.write.mode(SaveMode.Overwrite).parquet(s"${PropUtil.getValueByKey("HDFS.ROOT.162")}/lihaoran/YXFK/compute/${table}02")
          }
        })
        println(s"$table 写入HDFS完成，时间$time2")
        df.unpersist()
      } catch {
        case e: Exception =>
          println(s"$table 出现异常==========================\n")
          e.printStackTrace()
      }
    })
  }

  /*
  比如说更改数据库200万的操作时间
   */
  val 使用JDBC批量处理多个表 = 0
  if (0) {
    //注意表名之间不要加空格
    val tables = "SB_YXDNB_TEMP,KH_JLD_TEMP,KH_JSH_TEMP,KH_YDKH_TEMP"
    val db = "dfjs"
    //注意如果par并行数太多，会导致达到DB最大进程参数，报错ORA-12516, TNS:listener could not find available handler
    tables.split(",").par.foreach(table => {
      println("处理：" + table + "=============")
      JdbcUtil.execute(db, s"UPDATE $table SET CZSJ = to_date('2018-12-05 19:10:55','yyyy-mm-dd hh24:mi:ss') WHERE ROWNUM <= 2000000")
      //JdbcUtil.queryAndPrintH(db, s"select CZSJ from $table where rownum <= 10")
    })
  }

  val _________demo__________ = 0
  val 使用spark读取 = 0
  if (0) {
    /*
    集群中启动命令：
        /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 5g --executor-memory 8g --executor-cores 4  --num-executors 5  --master yarn-cluster --class sparkdemo.test.ReadDataBase --name dbtest-lhr --driver-class-path /usr/local/jar/lihaoran/ojdbc7.jar  --jars /usr/local/jar/lihaoran/ojdbc7.jar /usr/local/jar/lihaoran/lhrtest.jar
    注意：
    1、空指向异常：配置文件需要大写，否则读取不到
    2、--driver-class-path 以及 --jars 都要加上才能运行，否则会报错：java.lang.ClassNotFoundException: oracle.jdbc.OracleDriver
    3、Oracle读取的时候要左右加小括号，Mysql读取的时候要加自己的别名
      否则报错： Every derived table must have its own alias
     */

    //注意后面加个temp，这样Oracle和Mysql就可以通用了。
    //JdbcUtil.load("local", "(select ID,Name from student) temp",Array("ID='002'","ID='003'")).show()
    JdbcUtil.load("local", "(select ID,Name from student) temp", Array("Name='LHQ' and ID='002' ", "ID>='004'")).show()
    //JdbcUtil.load("local", "(select ID,Name from student where ID!='888') temp",Array("Name='LHQ' and ID='002' ","ID>='004'")).show()
    JdbcUtil.loadByColumn("local", "(select ID,Name from student) temp", "ID").show()
    //JdbcUtil.load("yxfk", "(select * from HS_DJBB) temp").show()

    //分区读取数据库
    //val (count2, time2) = getMethodRunTime(JdbcUtil.load("yxfk", "(select * from LC_YXDNBSS where rownum < 100000) temp").count()) //这种是数据库自己处理的
    //val (count1, time1) = getMethodRunTime(JdbcUtil.load("yxfk", "LC_YXDNBSS", Array("rownum < 100000")).count())
    //参考：https://www.jianshu.com/p/c18a8197e6bf
    //println(time1 + " " + time2) //2.24s 1.66s  //1.77s 1.88s
  }

  val 使用JDBC修改 = 0
  if (0) {
    if (0) {
      JdbcUtil.execute("local", "update student set Name='6666666' where ID='003'")
      JdbcUtil.queryAndPrintV("local", "select * from student")
    }
    if (0) {
      JdbcUtil.queryAndPrintH("yxfk", "select CZSJ from FW_YKYXBYQLSXX where rownum = 1")
      JdbcUtil.execute("yxfk", "UPDATE FW_YKYXBYQLSXX SET CZSJ = to_date('2018-11-27 19:10:55','yyyy-mm-dd hh24:mi:ss') WHERE ROWNUM = 1")
      JdbcUtil.queryAndPrintH("yxfk", "select CZSJ from FW_YKYXBYQLSXX where rownum = 1")
    }
  }

  val 使用JDBC读取 = 0
  if (0) {
    if (0) {
      //测试
      JdbcUtil.queryAndPrintV("gzdb", "select * from HS_DJBB where rownum = 1")
      //      JdbcUtil.queryAndPrintH("yxfk", "select * from FW_YKYXBYQLSXX where rownum = 1")
      //
      //      JdbcUtil.queryAndPrintH("local", "select * from student")
      //      val lst: List[Map[String, Any]] = JdbcUtil.queryAndWrap("local", "select * from student")
      //      JdbcUtil.queryAndPrintV("local", "select * from student")
    }
    if (0) {
      //读取大表的Schema信息
      val tables = "LC_YXDNBSS,SB_YXDNB,KH_JLD,ZW_SSDFJL,KH_JSH,KH_YDKH"
      tables.split(",").foreach(table => {
        println(s"${table}的Schema查询=======")
        JdbcUtil.getColumnsByJdbc("yxfk", s"select * from $table where rownum <= 1")
      })
    }
  }

}