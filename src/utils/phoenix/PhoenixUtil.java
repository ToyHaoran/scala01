package utils.phoenix;


import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import utils.PropUtil;

/**
 * Created with IntelliJ IDEA.
 * User: lihaoran
 * Date: 2018/9/17
 * Time: 11:01
 * Description: 全部是通过Phoenix来操作Hbase（删除了直接操作Hbase）
 * 使用phoenix-4.10.0.2.6.0.3-8-client.jar包。
 */
public class PhoenixUtil {
    //初始化容量,提高访问速度
    private static final int INITIAL_CAPACITY = 25;
    //phoenix地址
    private static final String URL = PropUtil.getValueByKey("PHOENIX.URL","app.properties");
    //1.获得Configuration实例并进行相关设置
    //2.获得Connection实例
    private static Connection connection;

    {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public void close() throws IOException {
        if (null != PhoenixUtil.connection) PhoenixUtil.connection.close();
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
