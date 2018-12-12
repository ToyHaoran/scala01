package utils.phoenix;


import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.phoenix.schema.PStringColumn;
import org.apache.phoenix.schema.types.PVarchar;
import utils.PropUtil;

/**
 * Created with IntelliJ IDEA.
 * User: lihaoran
 * Date: 2018/9/17
 * Time: 11:01
 * Description: Hbase详细工具类，用来直接操作Hbase以及使用Phoenix操作数据库。
 * 使用phoenix-4.10.0.2.6.0.3-8-client.jar包。
 */
public class HbaseUtil {
    //初始化容量,提高访问速度
    private static final int INITIAL_CAPACITY = 25;
    //phoenix地址
    private static final String URL = PropUtil.getValueByKey("PHOENIX.URL","app");
    //1.获得Configuration实例并进行相关设置
    //2.获得Connection实例
    private static Connection connection;
    private static Admin admin;

    {
        try {
            //如果不用Hbase，只用phoenix，那么用到的也就最后四个方法，其他的都没用
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        try {
            //以下代码是直接用于操作Hbase的，跳过了phoenix
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.client.ipc.pool.size", "1");
            configuration.addResource("");
            configuration.set("hbase.zookeeper.quorum", PropUtil.getValueByKey("HBASE.ZOOKEEPER.QUORUM","app"));
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.master", PropUtil.getValueByKey("HBASE.MASTER","app"));
            HbaseUtil.connection = ConnectionFactory.createConnection(configuration);
            //3.1获得Admin接口
            HbaseUtil.admin = HbaseUtil.connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }

    /**
     * 关闭连接和表管理对象
     */
    public void close() throws IOException {
        if (null != HbaseUtil.connection) HbaseUtil.connection.close();
        if (null != HbaseUtil.admin) HbaseUtil.admin.close();
    }

    /**
     * 创建表
     *
     * @param tableName   表名
     * @param familyNames 列族名
     */
    public void createTable(String tableName, String... familyNames) throws IOException {
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        }
        HbaseUtil.admin.createTable(tableDescriptor);
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public void dropTable(String tableName) throws IOException {
        //删除之前要将表disable
        if (!HbaseUtil.admin.isTableDisabled(TableName.valueOf(tableName))) {
            HbaseUtil.admin.disableTable(TableName.valueOf(tableName));
        }
        HbaseUtil.admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 指定行/列中插入数据
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param column    列
     * @param value     值
     */
    public void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 插入一行数据(未测试)
     *
     * @param tableName
     * @param rowKey
     * @param row       包括family，column，value
     * @throws IOException
     */
    public void insert(String tableName, String rowKey, List<List<String>> row) throws IOException {
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (List<String> item : row) {
            put.addColumn(Bytes.toBytes(item.get(0)), Bytes.toBytes(item.get(1)), Bytes.toBytes(item.get(2)));
        }
        table.put(put);
    }

    /**
     * 批量插入数据（未测试）
     *
     * @param tableName
     * @param rows      包括 rowkey,family,column,value
     * @throws IOException
     */
    public void insert(String tableName, List<List<String>> rows) throws IOException {
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(tableName));
        for (List<String> row : rows) {
            Put put = new Put(Bytes.toBytes(row.get(0)));
            put.addColumn(Bytes.toBytes(row.get(1)), Bytes.toBytes(row.get(2)), Bytes.toBytes(row.get(3)));
            table.put(put);
        }
    }

    /**
     * 删除表中的指定行
     *
     * @param tableName 表名
     * @param rowKeys   行键
     */
    public void delete(String tableName, String... rowKeys) throws IOException {
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(tableName));
        for (String rowkey : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
        }
    }

    /**
     * 根据rowKey family 获取指定列的数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param columns   某一列族中的列
     * @return list(String)
     * @throws IOException
     */
    public Map<String, byte[]> get(String tableName, String rowKey, String family, Object[] columns) throws IOException {
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(PVarchar.INSTANCE.toBytes(rowKey));
        byte[] familyBytes = PVarchar.INSTANCE.toBytes(family);
        get.addFamily(familyBytes);
        for (Object column : columns) {
            get.addColumn(familyBytes, PVarchar.INSTANCE.toBytes((String) column));
        }
        Result result = table.get(get);
        Map<String, byte[]> resultMap = new HashMap<String, byte[]>(HbaseUtil.INITIAL_CAPACITY);
        for (Object column : columns) {
            resultMap.put((String) column, result.getValue(familyBytes, PVarchar.INSTANCE.toBytes((String) column)));
        }
        return resultMap;
    }

    /**
     * 获取所有的列名称
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public List<String> columns(String tableName) throws IOException {
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(tableName));
        Filter filter = new KeyOnlyFilter();
        ResultScanner rs = table.getScanner(new Scan().setFilter(filter));
        List<String> column_list = new ArrayList<String>();
        Result result = rs.next();
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rsMap = result.getMap();
        for (byte[] bytes : rsMap.keySet()) {
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(bytes);
            for (byte[] kbs : familyMap.keySet()) {
                column_list.add(PVarchar.INSTANCE.toObject(kbs).toString());
            }
        }
        return column_list;
    }

    /**
     * 费控中用的，这里没用，先存着
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public List<String> getDays(String tableName) throws IOException {
        List<String> days_list = new ArrayList<String>();
        String daysTableName = "DAYS_" + tableName;
        Table table = HbaseUtil.connection.getTable(TableName.valueOf(daysTableName));
        ResultScanner rs = table.getScanner(new Scan());
        for (Result result : rs) {
            NavigableMap<byte[], byte[]> family_map = result.getFamilyMap(Bytes.toBytes("DAYS"));
            for (Entry<byte[], byte[]> entry : family_map.entrySet()) {
                days_list.add(Bytes.toString(entry.getValue()));
            }
        }
        return days_list;
    }

    /**
     * 根据表名，列族名，打印所有的column###value数据
     *
     * @param tableName
     * @param family
     * @param columns
     * @throws IOException
     */
    public void scan(TableName tableName, String family, String[] columns) throws IOException {
        byte[] familyBytes = Bytes.toBytes(family);
        Table table = null;
        try {
            table = HbaseUtil.connection.getTable(tableName);
            ResultScanner rs = null;
            try {
                //Scan scan = new Scan(Bytes.toBytes("u120000"), Bytes.toBytes("u200000"));
                Collection<Filter> filterList = new ArrayList<Filter>(HbaseUtil.INITIAL_CAPACITY);
                for (String columnStr : columns) {
                    Pattern compile = null;
                    CompareOp compareOp = null;
                    if (columnStr.contains("!=")) {
                        compile = Pattern.compile("=");
                        compareOp = CompareOp.NOT_EQUAL;
                    } else if (columnStr.contains("=")) {
                        compile = Pattern.compile("=");
                        compareOp = CompareOp.EQUAL;
                    } else if (columnStr.contains(">=")) {
                        compile = Pattern.compile(">=");
                        compareOp = CompareOp.GREATER_OR_EQUAL;
                    } else if (columnStr.contains(">")) {
                        compile = Pattern.compile(">");
                        compareOp = CompareOp.GREATER;
                    } else if (columnStr.contains("<=")) {
                        compile = Pattern.compile("<=");
                        compareOp = CompareOp.LESS_OR_EQUAL;
                    } else if (columnStr.contains("<")) {
                        compile = Pattern.compile("<");
                        compareOp = CompareOp.LESS;
                    }
                    if (null != compile) {
                        String[] columnAndValue = compile.split(columnStr);
                        byte[] qualifier = Bytes.toBytes(columnAndValue[0]);
                        byte[] value = Bytes.toBytes(columnAndValue[1]);
                        Filter filter = new SingleColumnValueExcludeFilter(familyBytes, qualifier, compareOp, value);
                        filterList.add(filter);
                    }
                }
                Scan scan = new Scan();
                for (Filter filter : filterList) {
                    scan = scan.setFilter(filter);
                }
                rs = table.getScanner(scan);
                for (Result r : rs) {
                    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = r.getMap();
                    for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()) {
                        NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
                        for (Entry<byte[], NavigableMap<Long, byte[]>> en : map.entrySet()) {
                            System.out.print("##" + Bytes.toString(en.getKey()));
                            NavigableMap<Long, byte[]> ma = en.getValue();
                            Set<Entry<Long, byte[]>> entries = ma.entrySet();
                            for (Entry<Long, byte[]> e : entries) {
                                System.out.print(e.getKey() + "###");
                                System.out.println(Bytes.toString(e.getValue()));
                            }
                        }
                    }
                }
            } finally {
                if (null != rs) rs.close();
            }
        } finally {
            if (null != table) {
                table.close();
            }
        }
    }

    /**
     * 执行任何sql
     *
     * @param sql
     * @param connection
     * @return
     * @throws SQLException
     */
    public static Boolean execute(String sql, java.sql.Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        Boolean flag = statement.execute(sql);
        //必须手动commit，否则不会执行
        connection.commit();
        statement.close();
        return flag;
    }

    /**
     * 执行查询语句
     *
     * @param sql        查询的SQL语句，例如：select * from xxxx limit 100
     * @param connection phoenix连接
     * @return resultSet 结果集
     * @throws SQLException
     */
    public static ResultSet query(String sql, java.sql.Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    /**
     * 获取连接
     *
     * @return phoenix连接
     * @throws SQLException
     */
    public static java.sql.Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL);
    }


    /**
     * 对jdbc的ResultSet进行封装
     *
     * @param resultSet 查询结果
     * @return list<HashMap>   每个HashMap是一行数据
     */
    public List<HashMap<String, Object>> dataWrap(ResultSet resultSet) throws SQLException {
        List<HashMap<String, Object>> buffer = new ArrayList<HashMap<String, Object>>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<String, Object>(INITIAL_CAPACITY);
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String key = resultSetMetaData.getColumnName(i);
                Object content = resultSet.getObject(i);
                map.put(key, content);
            }
            buffer.add(map);
        }
        return buffer;
    }

}
