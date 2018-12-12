package utils.phoenix

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import utils.PropUtil


object HbaseUtil2 {
  //1.获得Configuration实例并进行相关设置
  val configuration: Configuration = HBaseConfiguration.create
  configuration.set("hbase.zookeeper.quorum", PropUtil.getValueByKey("HBASE.ZOOKEEPER.QUORUM"))
  configuration.set("hbase.master", PropUtil.getValueByKey("HBASE.MASTER"))
  //2.获得Connection实例
  val connection: Connection = ConnectionFactory.createConnection(configuration)
  //3.1获得Admin接口
  val admin: Admin = connection.getAdmin
  //3.2获得Table接口,需要传入表名
  def tables(): Array[String] = admin.listTableNames().map(_.getNameAsString)

  /**
    * 创建表
    *
    * @param tableName   表名
    * @param familyNames 列族名
    **/
  @throws[IOException]
  def createTable(tableName: String, familyNames: String*): Unit = {
    if (admin.tableExists(TableName.valueOf(tableName))) return
    //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    for (familyName <- familyNames) {
      tableDescriptor.addFamily(new HColumnDescriptor(familyName))
    }
    admin.createTable(tableDescriptor)
  }

  /**
    * 删除表
    *
    * @param tableName 表名
    **/
  @throws[IOException]
  def dropTable(tableName: String): Unit = {
    //删除之前要将表disable
    if (!admin.isTableDisabled(TableName.valueOf(tableName))) admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))
  }


  /**
    * 指定行/列中插入数据
    *
    * @param tableName 表名
    * @param rowKey    主键rowkey
    * @param family    列族
    * @param column    列
    * @param value     值
    *                  TODO: 批量PUT
    */
  @throws[IOException]
  def insert(tableName: String, rowKey: String, family: String, column: String, value: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
  }

  def get(tableName: String, rowKey: String, family: String, columns: String*): Map[String, String] = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    if (family != null) {
      val familyBytes = Bytes.toBytes(family)
      get.addFamily(familyBytes)
      if (columns.nonEmpty)
        columns.foreach(column => get.addColumn(familyBytes, Bytes.toBytes(column)))
      val result = table.get(get)
      columns.par.map(x => (x, Bytes.toString(result.getValue(familyBytes, Bytes.toBytes(x))))).toArray.toMap
    } else
      throw new IllegalArgumentException("family name can not be null ...")

  }


  /**
    * 删除表中的指定行
    *
    * @param tableName 表名
    * @param rowKey    rowkey
    *                  TODO: 批量删除
    */
  @throws[IOException]
  def delete(tableName: String, rowKey: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val delete = new Delete(Bytes.toBytes(rowKey))
    table.delete(delete)
  }
}
