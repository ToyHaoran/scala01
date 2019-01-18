package utils.database

import java.util.Base64

import utils.database

object PropertyKey extends Enumeration {
  def main(args: Array[String]) {
    //对字符串进行加密，和二次加密
    val originString = "lihaoran"
    val encoder = Base64.getEncoder
    val s1 = new String(encoder.encode(encoder.encode(originString.getBytes())))
    println(s1)

    //二次解密
    val s2 = s1 //需要解密的字符串
    val decoder = Base64.getDecoder
    val s3 = new String(decoder.decode(decoder.decode(s2.getBytes())))
    println(s3)
  }
  type PropertyKey = Value
  val NAME: database.PropertyKey.Value = Value("NAME")
  val URL: database.PropertyKey.Value = Value("URL")
  val USER_NAME: database.PropertyKey.Value = Value("USER")
  //解密 todo 测试一下怎么用？
  /*val decoder = Base64.getDecoder
  var jmpassword = new String(decoder.decode(decoder.decode(Value("PASSWORD").toString.getBytes())))
  val PASSWORD: database.PropertyKey.Value = Value(jmpassword)*/
  val PASSWORD: database.PropertyKey.Value = Value("PASSWORD")
  val DRIVER: database.PropertyKey.Value = Value("DRIVER")
  val SWITCH: database.PropertyKey.Value = Value("SWITCH")
  val BATCHSIZE: database.PropertyKey.Value = Value("BATCHSIZE")
  val FETCHSIZE: database.PropertyKey.Value = Value("FETCHSIZE")
  val NUMPARTITIONS: database.PropertyKey.Value = Value("NUMPARTITIONS")
  val TRUNCATE: database.PropertyKey.Value = Value("TRUNCATE")
  val CREATE_TABLE_COLUMN_TYPES: database.PropertyKey.Value = Value("createTableColumnTypes")

  //检测是否存在枚举值
  def checkExists(key: String): Boolean = this.values.exists(_.toString == key)

  override implicit def toString(): String = super.toString()
}

