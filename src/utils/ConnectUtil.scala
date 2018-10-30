package utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ConnectUtil {

    /**
      * 得到本地SparkContext
      * @return
      */
    def getLocalSC: SparkContext ={
        val sc = new SparkContext(new SparkConf().setAppName("SparkContextDemo").setMaster("local[2]"))
        sc
    }

    /**
      * 得到本地SparkSession
      * @return
      */
    def getLocalSpark: SparkSession = {
        val spark = SparkSession.builder().appName("SparkSessionDemo")
            .config("spark.some.config.option", "some-value")
            .master("local")
            .getOrCreate()
        spark
    }

    /**
      * 得到集群的SparkSession
      * @return
      */
    def getClusterSpark: SparkSession = {
        val spark = SparkSession.builder().appName("SparkSessionDemo")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        spark
    }
}