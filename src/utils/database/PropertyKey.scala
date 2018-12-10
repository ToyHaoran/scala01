package utils.database

import java.util.Base64

import utils.database

object PropertyKey extends Enumeration {
    type PropertyKey = Value
    val NAME: database.PropertyKey.Value = Value("NAME")
    val URL: database.PropertyKey.Value = Value("URL")
    val USER_NAME: database.PropertyKey.Value = Value("USER")
    /*//解密
    val decoder = Base64.getUrlDecoder()
    var jmpassword = new String(decoder.decode("PASSWORD"))
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

