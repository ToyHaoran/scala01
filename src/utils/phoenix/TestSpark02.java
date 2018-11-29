package utils.phoenix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.ConnectUtil;

/**
 * Created with IntelliJ IDEA.
 * User: lihaoran
 * Date: 2018/11/14
 * Time: 8:40
 * Description:Java使用spark操作phoenix，一般不用。
 */
public class TestSpark02 {
    public static void main(String[] args) {
        SparkSession spark = ConnectUtil.spark();
        //本地读取数据
        Dataset<Row> ds = spark.read().format("jdbc")
                .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
                .option("url", "jdbc:phoenix:localhost:2181")
                .option("dbtable", "(select * from student)")
                .load();
        ds.show();//这里正常
        JavaRDD<Row> javaRDD =ds.javaRDD();// 不能直接foreach，没数据
        List<Row> list = javaRDD.collect();
        System.out.println("list size=================="+list.size());//有数据，为4
        List<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();
        //更加详细的数据处理见PhoenixUtil。
        for(int i = 0; i <list.size(); i++){
            Row row = list.get(i);
            HashMap<String, String> map = new HashMap<String, String>();
            map.put("ID",row.getString(0));
            map.put("NAME",row.getString(1));
            map.put("SEX",row.getString(2));
            map.put("AGE",String.valueOf(row.getInt(3)));
            result.add(map);
        }
        System.out.println("result=============="+result.size());
    }
}
