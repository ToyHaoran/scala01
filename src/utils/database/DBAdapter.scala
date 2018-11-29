package utils.database

import java.io.OutputStreamWriter
import java.util.Properties

import utils.{ConnectUtil, database}
import utils.BaseUtil._
import org.apache.spark.sql.{DataFrame, SaveMode}
import utils.database.PropertyKey._

import scala.collection.mutable

case class DBAdapter(properties: Properties) {

    private val dbProperties: Properties = new Properties()
    private val dbPropArr: Array[database.PropertyKey.Value] = Array(USER_NAME, PASSWORD, DRIVER)

    //将prop文件中的配置项添加到dbProperties中，key（小写）：value
    private val moveProperties = { (k: String) =>
        //读取的时候不区分大小写
        val low = k.toLowerCase
        val upper = k.toUpperCase
        val value = properties.getProperty(low)
        if(null == value){
            dbProperties.put(low, properties.getProperty(upper))
        }else{
            dbProperties.put(low, value)
        }
    }
    dbPropArr.foreach(x => moveProperties(x.toString))


    def save(data: DataFrame, table: String, saveMode: SaveMode, options: Map[String, String]): Unit = {
        val ops: mutable.Map[String, String] = mutable.Map()
        appendOptions(Array(NUMPARTITIONS, BATCHSIZE, TRUNCATE), ops)
        if (options.nonEmpty)
            options.foreach(x => ops.put(x._1.toLowerCase, x._2))
        val dataWriter = if (ops.isEmpty)
            data.write.mode(saveMode)
        else
            data.write.mode(saveMode).options(ops.toMap)
        dataWriter.jdbc(properties.getProperty(URL.toString), table, dbProperties)
    }

    def read(table: String, database: String, options: Map[String, String]): DataFrame = {
        val ops: mutable.Map[String, String] = mutable.Map()
        appendOptions(Array(NUMPARTITIONS, FETCHSIZE), ops)
        if (options.nonEmpty){
            options.foreach(x => ops.put(x._1.toLowerCase, x._2))
        }
        println(ops)
        ConnectUtil.spark.read.options(ops.toMap).jdbc(properties.getProperty(URL.toString), table, dbProperties)
    }

    def read(table: String, database: String, predicates: Array[String], options: Map[String, String]): DataFrame = {
        if (predicates.isEmpty)
            read(table, database, options)
        else {
            val ops: mutable.Map[String, String] = mutable.Map()
            appendOptions(Array(NUMPARTITIONS, FETCHSIZE), ops)
            if (options.nonEmpty)
                options.foreach(x => ops.put(x._1.toLowerCase, x._2))
            ConnectUtil.spark.read.options(ops.toMap)
                .jdbc(properties.getProperty(URL.toString), table, predicates, dbProperties)
        }
    }

    def put(k: AnyRef, v: AnyRef): Unit = {
        properties.put(k.toString, v.toString)
        if (dbPropArr.contains(k) || dbPropArr.map(_.toString).contains(k.toString))
            dbProperties.put(k.toString, v.toString)
    }

    def get(k: AnyRef): AnyRef = {
        properties.getOrDefault(k.toString.toUpperCase, null)
    }

    override def toString: String = {
        properties.toString
    }

    def write(filePath: String): Unit = {
        import java.io.FileOutputStream
        val fo = new FileOutputStream(filePath)
        properties.store(new OutputStreamWriter(fo, "utf-8"),
            properties.getProperty(NAME.toString, ""))
        fo.close()
    }

    def appendOptions(arr: Array[PropertyKey], ops: mutable.Map[String, String]): Map[String, String] = {
        arr.foreach(k => {
            val getPropOp = properties.getProperty(k.toString, "")
            if (!"".equals(getPropOp))
                ops.put(k.toString.toLowerCase, getPropOp)
        })
        ops.toMap
    }
}
