package utils.phoenix

import java.util.UUID

//下面两个需要连接Hbase的jar包：spark-hbase-connector-2.2.0-1.1.2-3.4.6.jar
import it.nerdammer.spark.hbase.PartitionMapper._
import it.nerdammer.spark.hbase._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StringType

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/30
  * Time: 9:04 
  * Description:
  */
object Save2Hbase {


}