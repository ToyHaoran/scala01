package utils.database

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData}
import java.util.concurrent.ConcurrentHashMap

import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SaveMode}
import utils.BaseUtil._
import utils.PropUtil
import utils.database.PropertyKey._

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
    * @return ResultSet
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
  def queryAndPrintH(database: String, sql: String): Unit = {
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
  def queryAndPrintV(database: String, sql: String): Unit = {
    queryAndWrap(database, sql).foreach(printMap(_))
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
    * 通过Spark加载Column信息
    */
  def getTableColumnsBySpark(db: String, table: String): Set[String] = {
    val sql = s"(SELECT * FROM $table WHERE ROWNUM = 1)"
    load(db, sql).columns.toSet
  }

  /**
    * 通过JDBC加载Column信息
    */
  def getTableColumnsByJdbc(database: String, sql: String): Array[String] = {
    val db = propertiesMap.get(database)
    val url = db.get(URL.toString).toString
    val user = db.get(USER_NAME.toString).toString
    val pass = db.get(PASSWORD.toString).toString
    val connection = getConnection(url, user, pass)
    val preparedStatement = connection.prepareStatement(sql)
    val resultSet = preparedStatement.executeQuery()
    val data: ResultSetMetaData = resultSet.getMetaData

    val colNames = ArrayBuffer[String]()
    for (i <- 1 to data.getColumnCount) {
      val columnName = data.getColumnName(i) //列的名称
      colNames += columnName
    }
    println(colNames.mkString(","))
    //关闭连接
    preparedStatement.close()
    connection.close()
    colNames.toArray
  }


  /**
    * 将DF保存到数据库
    *
    * @param database 数据库名称
    * @param table    表名
    * @param data     数据
    * @param saveMode 保存方式
    * @param options  自定义参数，优先级：自定义参数>配置文件>默认配置
    * @return 成功标志
    */
  def save(
              database: String, table: String, data: DataFrame, saveMode: SaveMode = SaveMode.Append,
              options: Map[String, String] = Map()): Unit = {
    val db = getDBAdapter(database)
    db.save(data, table, saveMode, options)
  }

  /**
    * 将DataFrame落入数据库，在费控中主要是用在mysql数据库上更新
    * 数据Save or update方法，只在Mysql中进行了测试
    * 原始数据（如果为时间类型）为空时，更新后时间为当前时间
    *
    * @param database  数据库名
    * @param table     表名
    * @param dataFrame 数据
    */
  def update(database: String, table: String, dataFrame: DataFrame): Unit = {
    val columns = dataFrame.columns.mkString(",")
    dataFrame.foreachPartition(partition => {
      val dbAdapter = getDBAdapter(database)
      val url = dbAdapter.get(URL.toString).toString
      val user = dbAdapter.get(USER_NAME.toString).toString
      val password = dbAdapter.get(PASSWORD.toString).toString

      val connection = getConnection(url, user, password)

      partition.foreach(r => {
        val schema: Seq[StructField] = r.schema //StructType(StructField(key1,StringType,true), StructField(key2,IntegerType,false), StructField(key3,IntegerType,false))
        val placeholders = schema.map(_ => "?").mkString(",") // ?,?,?  是占位符
        val duplicateSetting = schema.map(_.name).map(i => s"$i=?").mkString(",") // key1=?,key2=?,key3=?
        val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders) ON DUPLICATE KEY UPDATE $duplicateSetting"
        /*
        INSERT INTO student (key1,key2,key3) VALUES (?,?,?) ON DUPLICATE KEY UPDATE key1=?,key2=?,key3=?
        参考：https://9iphp.com/web/php/mysql-on-duplicate-key-update.html
        ON DUPLICATE KEY UPDATE只是MySQL的特有语法，并不是SQL标准语法！
        这个语法和适合用在需要 判断记录是否存在,不存在则插入存在则更新的场景
         */
        val preparedStatement = connection.prepareStatement(sql)
        val setters = Setter.getSetter(schema.toArray, connection, isUpdateMode = true)
        val setterLength = schema.length * 2 // 因为上面那个SQL语句有两倍的问号。
        (0 until setterLength).foreach(setterIndex => {
          val setter = setters(setterIndex) // 这是一个函数
          //注意这里判断是否是第二遍Index（后面的几个问号）
          val rowIndex = if (setterIndex < schema.length) setterIndex else setterIndex - schema.length
          if (r.get(rowIndex) != null) {
            setter(preparedStatement, r, setterIndex + 1, rowIndex) // 设置SQL语句
          } else {
            preparedStatement.setNull(setterIndex + 1, Setter.nullType(schema(rowIndex).dataType)) //如果为null，设置对应的null类型
          }
        })
        preparedStatement.execute()
        preparedStatement.close()
      })
      connection.close()
    })
  }


  /**
    * Spark通过JDBC加载数据方法封装
    *
    * @param database 数据库名称
    * @param table    表名
    * @param options  自定义参数，优先级：自定义参数>配置文件>默认配置
    * @return DataFrame
    */
  def load(database: String, table: String, predicates: Array[String] = Array(), options: Map[String, String] = Map()): DataFrame = {
    //没有此数据库会得到空指向异常，不需要处理，系统会打印错误信息
    val db = getDBAdapter(database)
    db.read(table, database, predicates, options)
  }


  /**
    * 带有条件的加载数据库表，(没有实现分区读取，和参数添加）
    *
    * @param database  数据库
    * @param table     表名
    * @param filed     字段名（一个str，中间用逗号分隔）
    * @param condition 条件
    * @return DF
    */
  def loadWithCondition(database: String, table: String, filed: String = "*", condition: Array[String] = Array("1=1")): DataFrame = {
    val db = getDBAdapter(database)
    val driver = db.properties.getProperty("DRIVER")
    val sql =
      if (driver.contains("oracle")) {
        s"(select $filed from $table where ${condition.mkString(" AND ")})"
      } else if (driver.contains("mysql")) {
        s"(select $filed from $table where ${condition.mkString(" AND ")}) AS t"
      } else {
        throw new Exception("未知数据库，请添加对应的处理条件")
        ""
      }
    db.read(sql, database, Array(), Map())
  }

  /**
    * 通过分区去加载数据库表
    *
    * @param database         数据库
    * @param table            表名
    * @param column           需要分区的列，如果表中没有此列，通过随机数分区加载。
    * @param defaultPartition 随机分区数，默认50
    * @return DF
    */
  def loadTable(database: String, table: String, column: String = "GDDWBM", defaultPartition: Int = 50): DataFrame = {
    val columns = getTableColumnsBySpark(database, table)
    if (columns.contains(column)) {
      val df = loadByColumn(database, table, column)
      df.cache()
      df.count()
      df
    } else {
      val df = loadByRandom(database, table, defaultPartition)
      df.cache() //必须加缓存，否则drop之后读取的数据为0,原因未知，估计是随机数又给删掉了。
      df.count()
      df.drop("RANDOMKEY")
    }
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
  def loadByColumn(database: String,
                   table: String,
                   classifyColumn: String,
                   ignoreNull: Boolean = false,
                   options: Map[String, String] = Map()): DataFrame = {
    val db = getDBAdapter(database)
    val predicates = PredicatesUtil.byColumn(database, table, classifyColumn, ignoreNull)
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
    * @param options    自定义参数，优先级：自定义参数>配置文件>默认配置
    * @return
    */
  def loadByDateBefore(database: String,
                       table: String,
                       dateColumn: String,
                       lastDate: java.sql.Date,
                       t: Int,
                       times: Int,
                       options: Map[String, String] = Map()): DataFrame = {
    val db = getDBAdapter(database)
    val predicates = PredicatesUtil.byDate(dateColumn, lastDate, t, times)
    db.read(table, database, predicates, options)
  }

  /**
    * 不需要列，直接并行读取。
    * 适合Oracle数据库。
    * 给定分区数，直接分区读取数据库。
    */
  def loadByRandom(database: String, table: String, numpartition: Int, options: Map[String, String] = Map()): DataFrame = {
    val db = getDBAdapter(database)
    val finalSql = s"(SELECT ROUND((DBMS_RANDOM.VALUE*$numpartition),0) AS RANDOMKEY,t.* from $table t)"
    val predicates = PredicatesUtil.byRandom("RANDOMKEY", numpartition)
    db.read(finalSql, database, predicates, options)
  }

  /**
    * 得到数据库适配器
    */
  private def getDBAdapter(database: String): DBAdapter = {
    propertiesMap.getOrDefault(database, null)
  }

  /**
    * 打印所有的数据库
    */
  def showAll(): Unit = {
    propertiesMap.values.toArray().foreach(println(_))
  }

  /**
    * 生成数据库配置文件信息，需要在配置文件中配置项目名称。
    * 感觉没什么卵用
    *
    * @param name     数据库名称
    * @param fileName 保存的配置文件名称
    * @return 成功标志
    */
  @deprecated
  private def writeProperties(name: String, fileName: String): Boolean = {
    val file = if (fileName.contains(".properties")) fileName else s"$fileName.properties"
    val db = propertiesMap.getOrDefault(name, null)
    if (db != null) {
      db.write(s"$PROJECT_ROOT_PATH/source/$file")
      true
    } else {
      false
    }
  }

}
