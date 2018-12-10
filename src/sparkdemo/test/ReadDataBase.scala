package sparkdemo.test

import utils.BaseUtil._
import utils.database.{JdbcUtil, PredicatesUtil}
import utils.{ConnectUtil, PropUtil}
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

    val 创建1亿条数据并存入数据库 = 0
    if (0) {
        /*
        集群启动命令：

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
                            .withColumn(key3, getUdf(col(key3))).na.drop(Seq(key1,key2,key3))
                }
            JdbcUtil.save(db, desTable, data, SaveMode.Append)
        }
        println("全部插入完成==============")

    }

    val 读取Oracle数据库速度测试 = 0
    if (1) {
        /*
        实现分段读取数据库，每个段读取少量的数据库，需要建立索引。
            1、读取KH_YDKH，总数据量112119565，增量200万
                在没有GDDWBM索引的情况下：3556.813s
                建立索引的情况下： 173s
            2、关于数据库参数对读取的影响：

        问题：
            1、查看数据库连接数：https://blog.csdn.net/zmx729618/article/details/54018629
         */
        //val tables = "LC_CBQDXX,XT_DMBM,HS_CQDLJL,HS_LTBZ,HS_BSBZ,FW_WQXX,HS_DJWDMX,HS_CQDLMX,ZW_FK_CSLJFMX,ZW_FK_YHDA,HS_DJBB,HS_DJDM,HS_DJMX,KH_JLD,KH_JSH,SB_YXDNB,KH_JLDGX,LC_YXDNBSS,ZW_FK_YHZZXX,DW_YXBYQ,KH_JLDBYQGX,HS_MFDYH,HS_JTBS,LC_HBXXJL,FW_YKJLDBYQGXLSXX,LC_JLDBGJL,XT_RY,XT_ZZ,XT_YXZZ,FW_KFGDXX,FW_GDYHGL,KH_df,ZW_SSDFJL,ZW_FK_HCYCJL,ZW_FK_YJGZD,ZW_FK_TFDGZD,ZW_FK_YCCBSJ,FW_YKJLDLSXX,FW_YKJLDGXLSXX,FW_YKYXBYQLSXX,ZW_FK_YCHQRZ,ZW_FK_CBXX,ZW_FK_CSJG,ZW_FK_SSYDXXHQJL"
        //val tables = "SB_YXDNB,KH_JLD,ZW_SSDFJL,KH_JSH,KH_YDKH" //600万以上的表 LC_YXDNBSS（没有GDDWBM）
        val db = "dfjs"
        val tables = "ZW_SSDFJL_TEMP" //KH_YDKH_TEMP
        val parTable = tables.split(",").par
        println(s"一共${parTable.size}个表")
        //存放表和时间
        val parMap = new java.util.concurrent.ConcurrentHashMap[String, String]()
        parTable.foreach(table => parMap.put(table, "0"))

        //时间较长的表  LC_YXDNBSS,SB_YXDNB,KH_JLD,ZW_SSDFJL,KH_JSH,KH_YDKH
        parTable.par.foreach(table => {
            try {
                var df:DataFrame = null
                val (count, time1) = getMethodRunTime({
                    /*val sql = s"(SELECT * FROM $table WHERE CZSJ > to_date('2018-12-04 18:31:30','yyyy-mm-dd hh24:mi:ss') AND CZSJ <= to_date('2018-12-04 23:59:59','yyyy-mm-dd hh24:mi:ss'))"
                    JdbcUtil.loadByColumn(db, sql, "GDDWBM").count()*/

                    df = JdbcUtil.loadByColumn(db, table, "GDDWBM")
                    /*
                    加缓存前读取时间：23.175s，写入时间：1464.584s
                    加缓存后读取时间：1597.801s ，写入时间：292.311s
                    总时间都差不多半小时。
                    说明还是读取的问题
                     */
                    /*df.cache()
                    df.show()*/
                    df.count()//有索引的情况下特别快
                })
                println(s"${table}数量：$count,读取需要时间：$time1 =========")
                parMap.remove(table)
                println("剩余：" + parMap.keys().mkString(","))

                val (res2, time2) = getMethodRunTime({
                    println(s"$table 写入HDFS开始=======")
                    df.write.mode(SaveMode.Overwrite).partitionBy("GDDWBM").parquet(s"${PropUtil.getValueByKey("HDFS.ROOT.162")}/YXFK/compute/$table")
                })
                println(s"$table 写入HDFS完成，时间$time2")
            } catch {
                case e: Exception =>
                    println("出现异常==========================\n"+ e.printStackTrace())
            }
        })
    }

    val 使用JDBC批量处理多个表 = 0
    if (0) {
        //注意表名之间不要加空格
        //val tables = "FW_YKYXBYQLSXX,ZW_FK_YCHQRZ,ZW_FK_CBXX,ZW_FK_CSJG,ZW_FK_SSYDXXHQJL,LC_CBQDXX,XT_DMBM"
        val tables = "SB_YXDNB_TEMP,KH_JLD_TEMP,KH_JSH_TEMP,KH_YDKH_TEMP"
        val db = "dfjs"
        //注意如果par并行数太多，会导致达到DB最大进程参数，报错ORA-12516, TNS:listener could not find available handler
        tables.split(",").par.foreach(table => {
            println("处理：" + table + "=============")
            JdbcUtil.execute(db, s"UPDATE $table SET CZSJ = to_date('2018-12-05 19:10:55','yyyy-mm-dd hh24:mi:ss') WHERE ROWNUM <= 2000000")
            //JdbcUtil.queryAndPrintH(db, s"select CZSJ from $table where rownum <= 10")
        })
    }

    val Demo_使用spark读取DB = 0
    if (0) {
        /*
        集群中启动命令：
            /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 5g --executor-memory 8g --executor-cores 4  --num-executors 5  --master yarn-cluster --class sparkdemo.test.ReadDataBase --name dbtest-lhr --driver-class-path /usr/local/jar/lihaoran/ojdbc7.jar  --jars /usr/local/jar/lihaoran/ojdbc7.jar /usr/local/jar/lihaoran/lhrtest.jar
        注意：
        1、空指向异常：配置文件需要大写，否则读取不到
        2、--driver-class-path 以及 --jars 都要加上才能运行，否则会报错：java.lang.ClassNotFoundException: oracle.jdbc.OracleDriver
         */

        //JdbcUtil.load("local", "(select ID,Name from student) as st").show()
        JdbcUtil.load("gzdb", "HS_DJBB").show()

        //分区读取数据库
        val (count2, time2) = getMethodRunTime(JdbcUtil.load("yxfk", "(select * from LC_YXDNBSS where rownum < 100000)").count()) //这种是数据库自己处理的
        val (count1, time1) = getMethodRunTime(JdbcUtil.load("yxfk", "LC_YXDNBSS", Array("rownum < 100000")).count())
        //参考：https://www.jianshu.com/p/c18a8197e6bf
        println(time1 + " " + time2) //2.24s 1.66s  //1.77s 1.88s
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
            //测试
            JdbcUtil.queryAndPrintH("gzdb", "select * from HS_DJBB where rownum = 1")
            JdbcUtil.queryAndPrintH("yxfk", "select * from FW_YKYXBYQLSXX where rownum = 1")

            JdbcUtil.queryAndPrintH("local", "select * from student")
            val lst: List[Map[String, Any]] = JdbcUtil.queryAndWrap("local", "select * from student")
            JdbcUtil.queryAndPrintV("local", "select * from student")
        }

        if (0) {
            //读取大表的Schema信息
            val tables = "LC_YXDNBSS,SB_YXDNB,KH_JLD,ZW_SSDFJL,KH_JSH,KH_YDKH"
            tables.split(",").foreach(table => {
                println(s"${table}的Schema查询=======")
                JdbcUtil.getTableColumnsByJdbc("yxfk", s"select * from ${table} where rownum <= 1")
            })
            /*
            LC_YXDNBSS的Schema查询=======
            YXDNBBS,JLDBH,SSLXDM,BSS,CBRQ,DQBM,CJSJ,CZSJ,JFGZDBH,DFNY,
            SB_YXDNB的Schema查询=======
            YXDNBBS,SBBS,ZCBH,CCBH,ZHBL,BMWSDM,FSWSDM,AZWZ,AZRQ,ZFBBZ,SCJYRQ,LHZQ,LHYXRQ,TXDZ1,TXDZ2,TXGYDM,BTL,TXFSDM,DQBM,XWDM,BXGSBBS,CBSXH,GDDWBM,CJSJ,CZSJ,XGNWZH,DHPCH,CSBS,SJZYBGSJ,YCBH,TZBZ,TZXE,YZJE,YJFZ1,YJFZ2,YJFZ3,TDFZ,FDYXJE,XYDJ,XYJDJ,XYFDJ,XYPDJ,XYGDJ,YXXMBS,JD,WD,BZ,CQGSDM,KGBZ,RGKZBZ,DKLX,DKJE,DKBL,DKKSSJ,DKJSSJ,SFKTYFFDK,SBYWZRBS,YWBZ,MJLXDM,MJ,SFAZCJDR,SFSXZCZS,FKKGXH,YXTXXY,YXTXFS,
            KH_JLD的Schema查询=======
            JLDBH,JSHH,YHBH,JFYXJ,JLDDZ,JLDMC,JLFSDM,JLDYDJDM,JLDYTDM,JLDWZDM,JXFSDM,ZXDJDFSDM,TYRQ,DYBH,DYZH,XLXDBS,TQBS,KGBH,JLZZFLDM,YDLBDM,DJDM,PJLXDM,CBFSDM,CBSXH,JLDZTDM,YGDDL,DLJSFSDM,DLDBZ,DBKJBZ,DLFTGS,BSFTFSDM,YGBSFTXYZ,WGBSFTXYZ,BSJFBZ,XSJSFSDM,XSJFBZ,XSFTBZ,XSFTXYZ,YGXSJSZ,WGXSJSZ,YDRL,SCECYJCSRQ,SCECFHCSRQ,PTBBDM,CTBBDM,GLYSBZDM,HYFLDM,FSJFBZ,JBDFJSFSDM,XLHDZ,GDLL,GLYSKHFSDM,DQBM,GNWZBM,GDDWBM,JLDLBDM,CJSJ,CZSJ,SWDLJLFX,JLDXH,SJZYBGSJ,JBDFFTFS,JBDFFTZ,BWTLX,DWDW,DCDW,XJJBDFJSFSDM,XJJBDFFTFS,XJJBDFFTZ,XJXLHDZ,TCDJDM,TCYXKSSJ,TCYXJSSJ,XJDJDM,JD,WD,
            ZW_SSDFJL的Schema查询=======
            YSZWLSH,SSZWLSH,JLDBH,JLDXH,GZDBH,CBQDBH,YHBH,JSHH,CBJHBH,DFNY,BQCBCS,YCBCS,JSLXDM,CZCS,CZNY,YXXBZ,YHMC,YDDZ,WYJRQ,HYFLDM,YHLBDM,PJLXDM,JFYXJ,CXDM,YDLBDM,YWLBDM,PJDYXXBS,DJDM,DJ,FSJFBZ,JFDL,YSDF,QF,QDDDF,QFJFHJ,QJBDF,QLTDF,YSWYJ,JSWYJRQ,QHZHXJE,QFZTDM,CLZTDM,XHBZ,SDRBS,SDWYJ,SDRQ,GDDWBM,DWBM,DQBM,SSLSH,SSSJ,SSRBS,SSZE,SSDF,SSDDDF,SSJBDF,SSLTDF,SSFJFHJ,SSHZHXJE,SSQT,SSWYJ,SSYS,DZSJ,DZBZ,BBNY,GXSJ,FXRBS,FXSJ,YYSZWLSH,YSSZWLSH,YJLDBH,ZJLXDM,JYFSDM,JFFSDM,SFRBM,DLSFRDM,DLSFLSH,DLSFSJ,JFPZDJH,PDHM,SSYHJFKYHDM,SFYHDM,YHRZLSH,YHKKPCH,GDFYHZH,YWZT,ZFRBS,ZFSJ,ZFYY,ZZRBS,ZZSJ,DZRQ,SJLX,CJSJ,CZSJ,DJBBBH,HZHXSJ,HZHXJE,KKKSRQ,QQT,JSHMC,JSHDZ,FPDYFS,FPHQFS,KKZH,JFQD,YCDZSJ,YCDZBZ,DLSFSBH,QZLX,DZWYBSM,ZZT,KKZHMC,RJQRBS,GRMZBZ,YHKBZ,BZ,JFDLX,CZRBS,YCBH,ZCBH,CCBH,JLDCBSXH,SSZKZYYS,LPZT,QDNDF,QSPDF,SSDNDF,SSSPDF,
            KH_JSH的Schema查询=======
            JSHH,KHBH,JSHMC,JSHDZ,JFFSDM,JFKH,FPDYFS,FPHQFS,DQYE,SDYE,YFXE,CDMSDM,DQBM,GDDWBM,CJSJ,CZSJ,ZDHBFSLX,KKLR,FPPSJGBM,ZDPDJGBM,QYTYXYDM,
            KH_YDKH的Schema查询=======
            YHBH,KHBH,YHMC,YDDZ,XYDJDM,XYFZ,JZDJDM,FXDJDM,YDLBDM,DYDJDM,HYFLDM,JLFSDM,YHLBDM,GDDWBM,CBQDBH,ZDYCXH,YYHBH,HTRL,YXRL,SCBCDM,FHXZDM,GHNHYLBDM,CXR,LHRQ,SDRQ,XHRQ,LSYDDQRQ,LSYDBZ,YHZTDM,YDJCZQ,SCJCRQ,JCQDBS,TDBZ,ZGLXDM,DQBM,DYLXDM,DYLSFSDM,DYQHFSDM,DYLSZZWZ,ZBDYBZ,ZBDYBSFSDM,ZBDYRL,XBYHBZ,CJFHYHBZ,GKKXBZ,XZQYDM,CXDM,SZLC,YFFLXDM,LSJFGXH,JCRYBS,CBSXH,DWTYDZ,YZBM,CZHM,KHSFDM,KHFQBZ,KHJLBS,SFYZBDC,ZBDCRL,BZFBZ,BZFZHS,BZFZMJ,BZFZRL,CJSJ,CZSJ,CBZQ,JTLX,CDM,SZXMBZ,SJZYBGSJ,YDJCSCZXJCRQ,GHRL,BSZDSJ,ZTQMSJ,FKMSDM,FFMSDM,SFYXTDBZ,TDLXDM,FDFSDM,JD,WD,SFLDHBZ,SCJYYHBZ,YHBZSXDM,YHTDFS,XYPJDF,XYPJDJ,XXCYDM,SFSLJJYH,
             */
        }

    }

    /**
      * 读取mysql数据库，废弃
      */
    @deprecated
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
    @deprecated
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
    @deprecated
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