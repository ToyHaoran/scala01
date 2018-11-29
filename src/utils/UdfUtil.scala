package utils

import java.util.UUID

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._ //导入UDF需要用
/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/12
  * Time: 19:09 
  * Description:
  */
object UdfUtil {


    //改变DF的Schema信息
    def intToLong: UserDefinedFunction = {
        udf((input: Int) => input.toLong)
    }
    def intToDouble: UserDefinedFunction = {
        udf((input: Int) => input.toDouble)
    }
    def longToDouble: UserDefinedFunction = {
        udf((input: Long) => input.toDouble)
    }

    /**
      * 将数值列中的空值转化为0
      * <p>
      * 例如：withColumn("Grade",nulltozero(col("Grade")))
      */
    def nulltozero: UserDefinedFunction = udf((input: String) =>
        //input.isEmpty是判断长度的
        if (null == input || input.trim().length == 0 || input.toLowerCase == "null") {
            0.0
        } else {
            input.toDouble
        }
    )

    /**
      * 拼接UUID
      * <p>
      * 例如：withColumn("PKID", getUUID())
      */
    def getUUID:UserDefinedFunction = udf { () =>
        UUID.randomUUID().toString.replaceAll("-", "").toUpperCase()
    }
}