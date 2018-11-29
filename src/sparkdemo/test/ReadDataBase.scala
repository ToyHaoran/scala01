package sparkdemo.test

import org.apache.spark.sql.SaveMode
import utils.BaseUtil._
import utils.{ConnectUtil, PropUtil}
import utils.database.JdbcUtil

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/22
  * Time: 14:01 
  * Description:
  */
object ReadDataBase extends App {

    val 批量处理多个表 = 0
    if (0) {
        //注意表名之间不要加空格
        val tables = "FW_YKYXBYQLSXX,ZW_FK_YCHQRZ,ZW_FK_CBXX,ZW_FK_CSJG,ZW_FK_SSYDXXHQJL,LC_CBQDXX,XT_DMBM"
        //注意如果par并行数太多，会导致达到DB最大进程参数，报错ORA-12516, TNS:listener could not find available handler
        tables.split(",") /*.par*/ .foreach(table => {
            println("处理：" + table + "=============")
            JdbcUtil.execute("yxfk", s"UPDATE $table SET CZSJ = to_date('2018-11-27 19:10:55','yyyy-mm-dd hh24:mi:ss') WHERE ROWNUM = 1")
            JdbcUtil.queryAndPrintH("yxfk", s"select CZSJ from $table where rownum = 1")
        })
    }

    val Demo_使用spark读取DB = 0
    if (1) {
        //空指向异常：配置文件需要大写，否则读取不到
        JdbcUtil.load("local", "(select ID,Name from student) as st").show()

        //JdbcUtil.load("gzdb", "HS_DJBB").show()
        //JdbcUtil.load("yxfk", "HS_DJBB").show()
    }

    val Demo_使用JDBC修改DB = 0
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

    val Demo_使用JDBC读取DB = 0
    if (0) {
        if (0) {
            JdbcUtil.queryAndPrintH("gzdb", "select * from HS_DJBB where rownum = 1")
        }

        if (0) {
            JdbcUtil.queryAndPrintH("yxfk", "select * from FW_YKYXBYQLSXX where rownum = 1")
        }

        if (0) {
            JdbcUtil.queryAndPrintH("local", "select * from student")
            val lst: List[Map[String, Any]] = JdbcUtil.queryAndWrap("local", "select * from student")
            JdbcUtil.queryAndPrintV("local", "select * from student")
        }
    }

    /**
      * 读取mysql数据库，废弃
      */
    private def old03: Unit = {
        val spark = ConnectUtil.spark

        val propUtil = PropUtil("localdb.properties")
        val url = propUtil.getConfig("url")
        //注意第二个参数是TABLE_NAME，相当于子查询，需要别名
        val table = "(select * from student) as st"
        //properties文件中的key必须是小写的这里的spark才能读到。
        val properties = propUtil.getProperties()

        val temp = spark.read.jdbc(url, table, properties)
        temp.show()
        temp.printSchema()

        //可以用来生成测试数据
        /*temp.repartition(2).write.mode(SaveMode.Overwrite).parquet("src/sparkdemo/testfile/temp")

        val temp02 = spark.read.parquet("src/sparkdemo/testfile/temp")
        temp02.show()
        temp02.printSchema()*/
    }

    /**
      * 读取Oracle数据库,废弃
      */
    private def old02: Unit = {
        val spark = ConnectUtil.spark

        val propUtil = PropUtil("yxfkdb")
        val url = propUtil.getConfig("URL")
        val table = "(select * from hs_djbb)"
        val dbProperties = propUtil.getProperties()

        // val oracleDemo = spark.read.jdbc(url, "(select * from hs_jldxx where rownum<=1000)", dataBaseProps)
        val temp = spark.read.jdbc(url, table, dbProperties)
        temp.show()
        temp.printSchema()
    }

    /**
      * 读取oracle  废弃
      */
    private def old01: Unit = {
        val spark = ConnectUtil.spark

        val url = getOracleUrl("xxx.xxx.xxx.xxx", "hydb")
        val user = "NWPMSKF"
        val password = "NWPMSKF"
        val dataBaseProps = new java.util.Properties()
        dataBaseProps.setProperty("user", user)
        dataBaseProps.setProperty("password", password)
        dataBaseProps.setProperty("fetchsize", "1000") //批量读
        dataBaseProps.setProperty("batchsize", "5000") //批量写

        // val oracleDemo = spark.read.jdbc(url, "(select * from hs_jldxx where rownum<=1000)", dataBaseProps)
        val temp = spark.read.jdbc(url, "(select * from hs_djbb)", dataBaseProps)
        temp.show()
        temp.printSchema()

        /*temp.write.parquet("hdfs://172.20.32.163:8020/temp_data/temp_lihaoran/temp")

        val temp02 = spark.read.parquet("hdfs://172.20.32.163:8020/temp_data/temp_lihaoran/temp")
        temp02.show()
        temp02.printSchema()*/
    }

    private def getOracleUrl(host: String, serviceName: String, port: String = "1521"): String = {
        //需要有读取Oracle的jar包
        //不知道为什么，简写版的连不上。  URL=jdbc:oracle:thin:@xxx.xxx.xxx.xxx:1521/hydb
        //"jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = xxx.xxx.xxx.xxx)(PORT = 1521)))(CONNECT_DATA=(SERVER = DEDICATED)(SERVICE_NAME = hydb)))"
        s"jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = $host)(PORT = $port)))(CONNECT_DATA=(SERVER = DEDICATED)(SERVICE_NAME = $serviceName)))"
    }
}