package utils.database

import java.sql.{Connection, PreparedStatement, Types}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * 用来填充SQL语句，使用方式见JdbcUtil.update
  */
object Setter {
    /**
      * 是一个函数：PreparedStatement，需要处理的行，sql中第几个问号，设置的值row.getInt(xxx)
      */
    private type JDBCValueSetter = (PreparedStatement, Row, Int, Int) => Unit

    def nullType(dataType: DataType): Int = {
        dataType match {
            case IntegerType => Types.INTEGER
            case LongType => Types.LONGVARCHAR
            case DoubleType => Types.DOUBLE
            case FloatType => Types.FLOAT
            case ShortType => Types.NUMERIC
            case ByteType => Types.VARCHAR
            case BooleanType => Types.BOOLEAN
            case StringType => Types.VARCHAR
            case BinaryType => Types.BINARY
            case TimestampType => Types.TIMESTAMP
            case DateType => Types.DATE
            case t: DecimalType => Types.DECIMAL
            case _ => throw new IllegalArgumentException(s"Can't translate type ${dataType.typeName} to database")
        }
    }

    /**
      * 得到每个一个设置SQl的函数数组
      * @return 注意：JDBCValueSetter是一个函数
      */
    def getSetter(fields: Array[StructField], connection: Connection, isUpdateMode: Boolean): Array[JDBCValueSetter] = {
        //StructField(key1,StringType,true)  //name,dataType,nullable
        val setter = fields.map(_.dataType).map(makeSetter(connection, _))
        if (isUpdateMode) {
            Array.fill(2)(setter).flatten
        } else {
            setter
        }
    }

    def makeSetter(conn: Connection, dataType: DataType): JDBCValueSetter = {
        dataType match {
            case IntegerType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setInt(pos, row.getInt(offset))
            case LongType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setLong(pos, row.getLong(offset))
            case DoubleType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setDouble(pos, row.getDouble(offset))
            case FloatType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setFloat(pos, row.getFloat(offset))
            case ShortType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setInt(pos, row.getShort(offset))
            case ByteType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setInt(pos, row.getByte(offset))
            case BooleanType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setBoolean(pos, row.getBoolean(offset))
            case StringType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setString(pos, row.getString(offset))
            case BinaryType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setBytes(pos, row.getAs[Array[Byte]](offset))
            case TimestampType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setTimestamp(pos, row.getAs[java.sql.Timestamp](offset))
            case DateType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setDate(pos, row.getAs[java.sql.Date](offset))
            case t: DecimalType =>
                (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) => stmt.setBigDecimal(pos, row.getDecimal(offset))
            case _ =>
                (_: PreparedStatement, _: Row, pos: Int, offset: Int) =>
                    throw new IllegalArgumentException(s"Can't translate non-null value for field $pos")
        }
    }
}
