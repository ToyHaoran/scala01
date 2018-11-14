package utils

import java.util.concurrent.ConcurrentHashMap

import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

class BroadcastUtil(spark: SparkSession) extends Serializable {

    private[this] val logger = Logger.getLogger(this.getClass)

    private[this] val broadcastVariables = new ConcurrentHashMap[String, Broadcast[_]]()

    /**
      * DataFrame转换成Map类型广播变量
      *
      * @param df
      * @param key
      * @param valueNames
      * @param keySpliter
      * @param valueSpliter
      * @return
      */
    def df2MapBroadCast(df: DataFrame, key: Array[String], valueNames: Array[String],
                        keySpliter: String, valueSpliter: String): Broadcast[_] = {
        import spark.implicits._
        val filelds: Array[String] = key.++(valueNames)
        val head: String = filelds.head
        val tail: Array[String] = filelds.tail
        val map = df.select(head, tail: _*).mapPartitions(partition => {
            partition.map { row => {
                val keys: StringBuilder = new StringBuilder
                key.foreach { k =>
                    keys.append(row.getAs(k).toString + keySpliter)
                }
                val mapKey: String = keys.toString().substring(0, keys.length - keySpliter.length)
                val vals: StringBuilder = new StringBuilder
                valueNames.foreach { vk =>
                    vals.append(row.getAs(vk).toString + valueSpliter)
                }
                val mapValue: String = if (valueSpliter == null || "".equals(valueSpliter)) {
                    vals.toString()
                } else {
                    vals.toString().substring(0, vals.length - valueSpliter.length)
                }
                ((mapKey, mapValue))
            }
            }
        }).rdd.collectAsMap()
        spark.sparkContext.broadcast(map)
    }

    /**
      * DataFrame转换成Set类型广播变量
      *
      * @param df
      * @param key
      * @param keySpliter
      * @return
      */
    def df2SetBroadCast(df: DataFrame, key: Array[String], keySpliter: String): Broadcast[_] = {
        import spark.implicits._
        val head: String = key.head
        val tail: Array[String] = key.tail
        val set = df.select(head, tail: _*).mapPartitions(part => {
            part.map { row =>
                val keys: StringBuilder = new StringBuilder
                key.foreach { k =>
                    keys.append(row.getAs(k).toString + keySpliter)
                }
                val setKey: String = keys.toString().substring(0, keys.length - keySpliter.length)
                (setKey)
            }
        }).rdd.collect().toSet
        spark.sparkContext.broadcast(set)
    }

    /**
      * 广播变量注册
      *
      * @param name
      * @param broadcast
      */
    def register(name: String, broadcast: Broadcast[_]): Unit = synchronized {
        if (!broadcastVariables.contains(name)) {
            broadcastVariables.put(name, broadcast)
        } else {
            logger.error("Key:$name already exists!")
            throw new RuntimeException(s"Key:$name already exists!")
        }
    }

    /**
      * DataFrame直接注册成Map类型广播变量
      *
      * @param name
      * @param df
      * @param key
      * @param valueNames
      * @param spliter
      */
    def register(name: String, df: DataFrame, key: Array[String], valueNames: Array[String], keySpliter: String,
                 spliter: String): Unit = {
        val broadcast = df2MapBroadCast(df, key, valueNames, keySpliter, spliter)
        register(name, broadcast)
    }

    /**
      * DataFrame直接注册成Set类型广播变量
      *
      * @param name
      * @param df
      * @param key
      * @param keySpliter
      */
    def register(name: String, df: DataFrame, key: Array[String], keySpliter: String): Unit = {
        val broadcast = df2SetBroadCast(df, key, keySpliter)
        register(name, broadcast)
    }

    /**
      * 获取广播变量，如果不存在则返回默认广播变量
      *
      * @param name
      * @param default
      * @return
      */
    def getOrDefault(name: String, default: Broadcast[_]): Broadcast[_] = {
        broadcastVariables.getOrDefault(name, default)
    }

    /**
      * 通过Key获取广播变量
      *
      * @param name
      * @return
      */
    def get(name: String): Option[Broadcast[_]] = {
        Option(broadcastVariables.getOrDefault(name, null))
    }

    def apply(name: String): Broadcast[_] = {
        try {
            broadcastVariables.get(name)
        } catch {
            case e: Exception =>
                throw new RuntimeException(s"Can Not find any value by the key:$name", e.getCause)
        }
    }

}

object BroadcastUtil extends Serializable {
    def getInstance(spark: SparkSession): BroadcastUtil = {
        new BroadcastUtil(spark)
    }
}
