package utils.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import utils.PropUtil;

/**
 * Created with IntelliJ IDEA.
 * User: lihaoran
 * Date: 2018/11/14
 * Time: 14:01
 * Description: 大大简化版的Phoenix查询工具类：获取连接，然后查询，然后封装结果。
 * 而且避免了与前端的jar包冲突，
 */
public class PhoenixQueryUtil {
    //private static final String URL = PropUtil.getValueByKey("PHOENIX.QUERY.URL","app");
    private static final String URL = "jdbc:phoenix:thin:url=http://172.20.32.211:8765;serialization=PROTOBUF";
    private static final String DRIVERNAME = "org.apache.phoenix.queryserver.client.Driver";
    private static Connection conn = null;

    /**
     * 获取链接
     * @return
     */
    private static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(DRIVERNAME);
            connection = DriverManager.getConnection(URL);
            connection.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }


    /**
     * sql查询
     * @param sql
     * @return 一个list是一个表，一个HashMap是一条记录（字段：值）
     */
    public static List<HashMap<String, Object>> query(String sql) {
        List<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        if(conn == null) {
            conn = getConnection();
        }
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            result = wrapResultSet(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(rs != null && !rs.isClosed()) {
                    rs.close();
                    rs = null;
                }
                if(ps != null && !ps.isClosed()) {
                    ps.close();
                    ps = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 查询结果封装
     * @param rs
     * @return 将每一条记录封装为一个HashMap，然后多条记录组合为一个list
     */
    private static List<HashMap<String, Object>> wrapResultSet(ResultSet rs) {
        List<HashMap<String, Object>> result = new ArrayList<HashMap<String, Object>>();
        ResultSetMetaData md;
        try {
            md = rs.getMetaData();
            int columnCount = md.getColumnCount();
            while (rs.next()) {
                HashMap<String,Object> rowData = new HashMap<String,Object>();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(md.getColumnName(i), rs.getObject(i));
                }
                result.add(rowData);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

}