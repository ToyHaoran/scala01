package utils

import org.apache.spark.{SparkConf, SparkContext}

object ConnectUtil {

    def getSparkLocal: SparkContext ={
        val sc = new SparkContext(new SparkConf().setAppName("demo").setMaster("local[2]"))
        sc
    }
}