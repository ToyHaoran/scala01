package utils.database

import java.io.InputStreamReader
import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData}
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import utils.PropUtil
import utils.BaseUtil._
import org.apache.spark.sql.{DataFrame, SaveMode}
import utils.database.PropertyKey._
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructField

import scala.collection.mutable.ArrayBuffer


object JdbcUtil {

    private val PROJECT_ROOT_PATH = PropUtil.getValueByKey("PROJECT.ROOT.PATH")

    //存放数据库的map
    private val propertiesMap: ConcurrentHashMap[String, DBAdapter] = new ConcurrentHashMap[String, DBAdapter]()

    PropUtil.getValueByKey("DATABASE.PATH").split(",").foreach(property => {
        val propUtil = PropUtil(property)
        val prop = propUtil.getProperties()
        val name = propUtil.getConfig("name")

        //提示信息
        Logger.getLogger("org.apache.spark").warn("Loading database  " + name)

        //增加数据库
        propertiesMap.put(name, DBAdapter(prop))
    })

    private def getConnection(url: String, user: String, pass: String): Connection = {
        DriverManager.getConnection(url, user, pass)
    }

    /**
      *
      * 对指定数据库执行指定sql语句
      *
      * @param database 数据库名称
      * @param sql      执行语句
      */
    def execute(database: String, sql: String): Unit = {
        val db = propertiesMap.get(database)
        val url = db.get(URL.toString).toString
        val user = db.get(USER_NAME.toString).toString
        val pass = db.get(PASSWORD.toString).toString
        val connection = getConnection(url, user, pass)
        val preparedStatement = connection.prepareStatement(sql)
        preparedStatement.execute()
        preparedStatement.close()
        connection.close()
    }

    /**
      * 对指定数据库执行相应查询语句，查询小批量数据时使用
      *
      * @param database 所查询数据库
      * @param sql      查询语句
      * @return         ResultSet
      */
    private def query(database: String, sql: String): ResultSet = {
        val db = propertiesMap.get(database)
        val url = db.get(URL.toString).toString
        val user = db.get(USER_NAME.toString).toString
        val pass = db.get(PASSWORD.toString).toString
        val connection = getConnection(url, user, pass)
        val preparedStatement = connection.prepareStatement(sql)
        val resultSet = preparedStatement.executeQuery()
        preparedStatement.close()
        connection.close()
        resultSet
    }

    /**
      * 查询并且横着打印信息
      */
    def queryAndPrintH(database: String, sql: String): Unit ={
        val db = propertiesMap.get(database)
        val url = db.get(URL.toString).toString
        val user = db.get(USER_NAME.toString).toString
        val pass = db.get(PASSWORD.toString).toString
        val connection = getConnection(url, user, pass)
        val preparedStatement = connection.prepareStatement(sql)
        val resultSet = preparedStatement.executeQuery()

        val data: ResultSetMetaData = resultSet.getMetaData
        while (resultSet.next()) {
            for (i <- 1 to data.getColumnCount) {
                if (i > 1) print(",  ")
                val value = resultSet.getString(i)
                val key = data.getColumnName(i)
                print(key + ":" + value)
            }
            println("")
        }

        //关闭连接
        preparedStatement.close()
        connection.close()
    }

    /**
      * 查询并且竖着打印信息
      */
    def queryAndPrintV(database: String, sql: String): Unit ={
        queryAndWrap(database,sql).foreach(printMap(_))
    }

    /**
      * 查询并对jdbc的ResultSet进行封装
      */
    def queryAndWrap(database: String, sql: String): List[Map[String, Any]] = {
        val db = propertiesMap.get(database)
        val url = db.get(URL.toString).toString
        val user = db.get(USER_NAME.toString).toString
        val pass = db.get(PASSWORD.toString).toString
        val connection = getConnection(url, user, pass)
        val preparedStatement = connection.prepareStatement(sql)
        val resultSet = preparedStatement.executeQuery()


        val buffer: ArrayBuffer[Map[String, Any]] = new ArrayBuffer[Map[String, Any]]()
        val data: ResultSetMetaData = resultSet.getMetaData
        while (resultSet.next()) {
            var map: Map[String, Any] = Map()
            for (i <- 1 to data.getColumnCount) {
                val key: String = data.getColumnName(i)
                val content: Any = resultSet.getObject(key)
                map += (key -> content)
            }
            buffer.+=(map)
        }

        //关闭连接
        preparedStatement.close()
        connection.close()

        buffer.toList
    }



    /**
      * 落库方法封装
      *
      * @param database 数据库名称
      * @param table    表名
      * @param data     数据
      * @param saveMode 保存方式
      * @param options  自定义参数，优先级：自定义参数>配置文件>默认配置
      * @return 成功标志
      */
    def save(database: String, table: String, data: DataFrame, saveMode: SaveMode = SaveMode.Append,
             options: Map[String, String] = Map()): Unit = {
        val db = getDBAdapter(database)
        db.save(data, table, saveMode, options)
    }

    /**
      * 数据Save or update方法，只在Mysql中进行了测试
      * 原始数据（如果为时间类型）为空时，更新后时间为当前时间
      *
      * @param database  数据库名
      * @param table     表名
      * @param dataFrame 数据
      */
    def update(database: String, table: String, dataFrame: DataFrame): Unit = {
        val columns = dataFrame.columns.mkString(",")
        dataFrame.foreachPartition(
            partition => {
                val dbAdapter = getDBAdapter(database)
                val connection = getConnection(dbAdapter.get(URL.toString).toString,
                    dbAdapter.get(USER_NAME.toString).toString, dbAdapter.get(PASSWORD.toString).toString
                )
                partition.foreach(
                    r => {
                        val schema: Seq[StructField] = r.schema
                        val placeholders = schema.map(_ => "?").mkString(",")
                        val duplicateSetting = schema.map(_.name).map(i => s"$i=?").mkString(",")
                        val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders) ON DUPLICATE KEY UPDATE $duplicateSetting"
                        val preparedStatement = connection.prepareStatement(sql)
                        val setters = Setter.getSetter(schema.toList.toArray, connection, isUpdateMode = true)
                        val setterLength = schema.length * 2
                        (0 until setterLength).foreach(
                            setterIndex => {
                                val setter = setters(setterIndex)
                                val rowIndex = if (setterIndex < schema.length) setterIndex else setterIndex - schema.length
                                if (r.get(rowIndex) != null)
                                    setter(preparedStatement, r, setterIndex + 1, rowIndex)
                                else
                                    preparedStatement.setNull(setterIndex + 1, Setter.nullType(schema(rowIndex).dataType))
                            }
                        )
                        preparedStatement.execute()
                        preparedStatement.close()
                    }
                )
                connection.close()
            }
        )
    }


    /**
      * Spark通过JDBC加载数据方法封装
      *
      * @param database 数据库名称
      * @param table    表名
      * @param options  自定义参数，优先级：自定义参数>配置文件>默认配置
      * @return  DataFrame
      */
    def load(database: String, table: String, predicates: Array[String] = Array(),
             options: Map[String, String] = Map()): DataFrame = {
        //没有此数据库会得到空指向异常，不需要处理，系统会打印错误信息
        val db = propertiesMap.getOrDefault(database, null)
        db.read(table, database, predicates, options)
    }

    /**
      * Spark通过JDBC,基于分区列并行加载数据方法封装
      *
      * @param database       数据库名称
      * @param table          表名
      * @param classifyColumn 分类型变量列
      * @param ignoreNull     是否忽略 classifyColumn=null的数据
      * @param options        自定义参数，优先级：自定义参数>配置文件>默认配置
      * @return
      */
    def loadByColumn(database: String, table: String, classifyColumn: String,
                     ignoreNull: Boolean = false, options: Map[String, String] = Map()): DataFrame = {
        val db = getDBAdapter(database)
        val predicates = PredicatesUtil.predicates(database, table, classifyColumn, ignoreNull)
        db.read(table, database, predicates, options)
    }

    /**
      * Spark通过JDBC,基于分区列并行加载数据方法封装
      *
      * @param database   数据库名称
      * @param table      表名
      * @param dateColumn 日期类型变量列
      * @param lastDate   开始时期
      * @param t          多少天作为一个分区的周期
      * @param times      周期数
      * @param ignoreNull 是否忽略 classifyColumn=null的数据
      * @param options    自定义参数，优先级：自定义参数>配置文件>默认配置
      * @return
      */
    def loadByDateBefore(database: String, table: String, dateColumn: String,
                         lastDate: java.sql.Date, t: Int, times: Int, ignoreNull: Boolean = false,
                         options: Map[String, String] = Map()): DataFrame = {
        val db = getDBAdapter(database)
        val predicates = PredicatesUtil.predicates(dateColumn, lastDate, t, times)
        db.read(table, database, predicates, options)
    }

    private def getDBAdapter(database: String): DBAdapter = {
        val db = propertiesMap.getOrDefault(database, null)
        db
    }

    def checkAvailable(database: String): Boolean = {
        val db = getDBAdapter(database)
        if (null != db) true else false
    }

    final case class DatabaseUnavailableException(private val msg: String, private val cause: Throwable = None.orNull)
        extends Exception(msg, cause)

    def showAll(): Unit = propertiesMap.values.toArray().foreach(println(_))

    /**
      * 生成配置文件信息，需要在配置文件中配置项目名称
      *
      * @param name     数据库名称
      * @param fileName 保存对配置文件名称
      * @return 成功标志
      */
    def writeProperties(name: String, fileName: String): Boolean = {
        val file = if (fileName.contains(".properties")) fileName else s"$fileName.properties"
        val db = propertiesMap.getOrDefault(name, null)
        if (db != null) {
            db.write(s"$PROJECT_ROOT_PATH/source/$file")
            true
        }
        else
            false
    }

}
