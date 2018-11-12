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

    /**
      * 将数值列中的空值转化为0
      * @return
      */
    def nulltozero: UserDefinedFunction = udf((input: String) =>
        //input.isEmpty是判断长度的
        if(null == input || "" == input.trim()){
            0.toDouble
        }else{
            input.toDouble
        }
    )

    /**
      * 拼接UUID
      */
    def appendUUID:UserDefinedFunction = udf { () =>
        UUID.randomUUID().toString.replaceAll("-", "").toUpperCase()
    }





}