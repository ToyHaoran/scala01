package utils.phoenix;

import java.sql.*;
import java.util.HashMap;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: lihaoran
 * Date: 2018/9/13
 * Time: 15:01
 * Description:通过phoenix读取Hbase数据。
 */
public class TestHbaseUtil {
    public static void main(String[] args) throws Exception {
        if(false){
            //测试phoenix查询数据
            HbaseUtil hbaseUtil = new HbaseUtil();
            Connection conn = HbaseUtil.getConnection();
            ResultSet resultSet = HbaseUtil.query("select * from BMSD limit 100", conn);
            List<HashMap<String, Object>> result = hbaseUtil.dataWrap(resultSet);
            System.out.println("result.size===================" + result.size());
            System.out.println(result.get(2).get("UUID"));
        }

        if(false){
            //测试phoenix查询数据另一个集群
            HbaseUtil hbaseUtil = new HbaseUtil();
            Connection conn = HbaseUtil.getConnection();
            ResultSet resultSet = HbaseUtil.query("select * from ZW_FK_RESULT limit 100", conn);
            List<HashMap<String, Object>> result = hbaseUtil.dataWrap(resultSet);
            System.out.println("result.size===+++================" + result.size());
        }
    }
}