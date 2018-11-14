package utils.phoenix;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
/**
 * Created with IntelliJ IDEA.
 * User: lihaoran
 * Date: 2018/11/14
 * Time: 11:34
 * Description: java版用JDBC操作phoenix
 */
public class ReadUseJdbc {
    public static void main(String[] args) throws SQLException {
        String sql = "SELECT * FROM STUDENT";
        List<HashMap<String, Object>> result = PhoenixUtil.query(sql);
        System.out.println(result.size());
    }
}