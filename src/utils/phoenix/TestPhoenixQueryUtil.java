package utils.phoenix;

import java.sql.*;
import java.util.HashMap;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: lihaoran
 * Date: 2018/11/14
 * Time: 11:34
 * Description: java版用JDBC操作phoenix
 */
public class TestPhoenixQueryUtil {
    /**
     * 这个是简化版的Phoenix查询，只能用来查询，而且需要开启的服务是QueryServer.
     */
    public static void main(String[] args) throws SQLException {
        String sql = "SELECT * FROM BMSD limit 100";
        List<HashMap<String, Object>> result = PhoenixQueryUtil.query(sql);
        System.out.println(result.size());

        /*
        本机和集群报错：
        java.sql.SQLException: while preparing SQL: SELECT * FROM BMSD limit 100
        Caused by: java.lang.RuntimeException: response code 500
        估计是因为Spark自带的calcite-avatica-1.2.0-incubating.jar与Phoenix的1.10.0版本冲突，前者不支持PROTOBUF。
        解决方式，不使用spark，只是使用phoenix-4.10.0.2.6.0.3-8-thin-client.jar这一个包。
        实际使用是在前端直接查询，并不会用到spark.
         */

        /*
        集群启动命令：
        /usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --driver-memory 5g --executor-memory 5g --executor-cores 3  --num-executors 5  --master yarn-cluster --class utils.phoenix.TestPhoenixQueryUtil --name hbase-lhr --jars /usr/local/jar/lihaoran/phoenix-4.10.0.2.6.0.3-8-thin-client.jar,/usr/local/jar/lihaoran/spark-hbase-connector-2.2.0-1.1.2-3.4.6.jar  /usr/local/jar/lihaoran/lhrtest.jar
         */
    }
}