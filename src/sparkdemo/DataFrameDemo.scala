package sparkdemo

import org.apache.spark.sql.expressions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.BaseUtil._
import utils.UdfUtil._
import utils.ConnectUtil

object DataFrameDemo extends App {

    val sc = ConnectUtil.sc
    //1.6后基本上不用sc了，用spark.sparkContext代替
    val spark = ConnectUtil.spark

    import spark.implicits._

    val df = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("bbb", 1, 5), ("bbb", 2, 1), ("ccc", 4, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")

    case class Person(name: String, age: Int)

    val people = sc.textFile("src/sparkdemo/testfile/person.txt")
        .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF() //转化为df后，统统为Row类型

    val DF打印操作 = 0
    if (0) {
        //打印的时候对不齐没事。
        df.show(30) //最多打印30行
        df.show(30, truncate = 20) // 每个元素最大20个字符，其他折叠
        df.show(30, truncate = false) // 每个元素不折叠

        //如果是并行打印，show会占用一点时间，导致print同时打印，然后show也打印在了一起。
        //这种情况下，推荐println("")放在show后面

        //打印Schema信息，常用于检查数据格式
        df.printSchema()

        //获取key的数目，常用于调整数据倾斜   //参考 src.utils.BaseUtil
        df.printKeyNums("key1")
    }

    val 创建DF的方式 = 0
    if (0) {
        //需要先导入隐式操作(否则toDF等快捷函数不能用)
        import spark.implicits._
        //方法一，Spark中使用toDF函数创建DataFrame，如果不指定列名字，默认为"_1","_2"..........
        val df = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")
        val df_1 = Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6)).toDF("key1", "key2", "key3") //同上
        //通过case class + toDF创建DataFrame的示例：开头的people的创建

        //方法二，Spark中使用createDataFrame函数从RDD创建DataFrame
        import org.apache.spark.sql.types._
        val schema = StructType(List(
            StructField("integer_column", IntegerType, nullable = false),
            StructField("string_column", StringType, nullable = true),
            StructField("date_column", DateType, nullable = true)
        ))
        val rdd = sc.parallelize(Seq(
            //注意是Row类型
            Row(1, "First Value", java.sql.Date.valueOf("2010-01-01")),
            Row(2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
        ))
        val df3 = spark.createDataFrame(rdd, schema)
        df3.printSchema()

        //方法三，通过文件直接创建DataFrame(推荐)
        spark.read.parquet("hdfs://xxx.xxx.xxx.xxx:8020/path/to/file") //使用parquet文件创建
        spark.read.parquet("E:/hivedata1/xxxx")
        spark.sql("SELECT * FROM parquet.'/src/xxx/xxxx/users.parquet'") //直接在文件上运行SQL
        spark.read.json("examples/src/main/resources/people.json") //使用json文件创建

        //通用加载保存功能
        spark.read.format("csv") //使用csv文件,spark2.0+之后,通用加载json, parquet, jdbc, orc, libsvm, csv, text
            .option("sep", ",")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("file:/usr/local/jar/lihaoran/测试.csv")
            //.load("sparkdemo/testfile/测试.csv")
            .show()

        //从Hive中读取
        //Use SparkSession.builder.enableHiveSupport instead
        //详细参考文档：http://spark.apachecn.org/docs/cn/2.2.0/sql-programming-guide.html
        spark.read.table("student")
        df.write.saveAsTable("student")

        //通过JDBC加载数据并创建df，参考：sparkdemo.test.ReadDataBase
        val url: String = "jdbc:oracle:thin:@xxx.xxx.xxx.xxx:1521:dyjc"
        val dataBaseProps = new java.util.Properties()
        dataBaseProps.setProperty("user", "xxx")
        dataBaseProps.setProperty("password", "xxxxx")
        val res = spark.read.jdbc(url, "(select * from lc_cbxx where rownum<=1000)", dataBaseProps) //write是一样的格式

        df.write.format("jdbc")
            .option("url","jdbc:dialect:serverName")
            .option("user","user")
            .option("password","pass")
            .option("dbtable","table").save()
    }

    val RDD_DF_DS的相互转化 = 0
    if (0) {
        //——————————————————————————RDD、DF、DS的相互转化————————————————————————————————————————————————
        //RDD转化为DF，参考创建DF方式1，2，一种是直接map到Person，然后toDF；一种是case Class，然后createDF
        //RDD转化为DS,同DF，不需要创建Schema
        //spark.createDataset(可选Seq、List、RDD(T))

        //DF转化为RDD,全部是Row类型,需要自己在重新map一下
        df.rdd.map(x => (x.getString(0), x.getInt(1), x.getInt(2)))
        //DF转化为DS
        val ds = people.as[Person]

        //DS转化为RDD,带Person类型，比DF更好
        ds.rdd.map(x => (x.name, x.age))
        //DS转化为DF
        ds.toDF() //带有name，age
        ds.toDF("k1", "k2") //更改Schema

        //DS操作
        ds.map(person => {
            val name = person.name
            val age = person.age + 10
            (name, age)
        })
        ds.select($"name".as[String],($"age">10).as[Boolean]).toDF()
    }

    val DF数据持久化 = 0
    if (0) {
        //——————————————————————————df数据持久化———————————————————————
        df.cache() //等价于下面的
        df.persist() //默认MEMORY_AND_DISK
        //持久化级别
        import org.apache.spark.storage.StorageLevel
        df.persist(StorageLevel.DISK_ONLY)
        df.persist(StorageLevel.MEMORY_ONLY)
    }

    val SQL风格语法 = 0
    if (0) {
        //——————————————————————————SQL风格语法————————————————————————————————
        //如果想使用SQL风格的语法，需要将DataFrame注册成临时视图
        people.createOrReplaceTempView("people")
        //查询年龄最大的前两名
        spark.sql("select * from people order by age desc limit 2").show
        //显示表的Schema
        spark.sql("desc people").show

        //全局临时视图
        //全局临时视图与系统保留的数据库global_temp绑定，我们必须使用限定名称来引用它，例如SELECT * FROM global_temp.view1。
        df.createGlobalTempView("people")
        spark.sql("SELECT * FROM global_temp.people").show()
        spark.newSession().sql("SELECT * FROM global_temp.people").show() //跨会话

    }

    val DF对RDD常用算子的支持 = 0
    if (0) {
        //—————————————————————————df对RDD常用算子的支持——————————————————————————————————
        df.map(row => {
            //DF中的每一个元素都是Row类型
            val r1 = row.getString(0) + " " * 2
            val r2 = row.getInt(1) * 2
            val r3 = " " + row.get(2).toString + " "
            //返回DataSet(默认_1,_2),不能是Row类型
            (r1 + r3, r2) //或Person(r1,r2)
        })

        val hello = spark.read.textFile("src/sparkdemo/testfile/hello.txt").as[String]
        val helloDS = hello.flatMap(_.split("\\s+")).filter(_.length > 0).map(word => (word, 1))
        //接下来，不能用helloDS.reduce(reduceFunc(_+_))（因为它只返回一个值(String,Int),无法处理不同的结果），
        //!!!并且DS也没有reduceByKey方法

        //可以用以下操作：
        helloDS.groupBy($"_1").count().show() //DS的分组，计数，agg等函数操作
        //或者参考练习题中——例题4的DF操作。
        helloDS.rdd.reduceByKey(_ + _).collect() //转化为RDD然后进行ReduceByKey操作
        helloDS.createOrReplaceTempView("hello") //或者注册为view再进行sql操作
        spark.sql("select _1 as name,count(*) as num from hello group by _1 ")

    }

    val 对Column的操作 = 0
    if (0) {
        //—————————————————————————对Column的操作—————————————————————————
        //select三种调用方法
        //通过键选择
        df.select("key1") //功能最少，不推荐使用
        df.select($"key1") //推荐，注意$"_"返回Column对象
        df.select($"key2" + 1) //推荐
        df.select(col("key2") + 1) //推荐
        df.col("key1")
        df.apply("key2")
        df.select(df.col("key1")) // 常用于两表连接后有相同字段时，选择某个表的一个字段
        df.select("key1", "key2")

        //在不知道列名的情况下查询第一列,第二列，第三列
        df.printSchema()
        df.select(df.schema.head.name)
        df.select(df.schema.apply(1).name) //key2
        df.select(df.schema.last.name)

        // df.select(upper("key1"))//报错
        df.select(upper($"key1"))
        df.select(upper(df.col("key1")))

        //selectExpr更强大，能使用sql函数
        df.selectExpr("upper(key1)", "key2 as haha2").show
        df.select(expr("upper(key1)"))
        df.select(expr("key2 + 1").as[Int])

        //添加，修改，删除列操作（select中用when函数也可以，但是功能较少，被UDF取代）
        df.withColumn("key4", lit(1024)) //添加列key5，初始值为1024
        df.withColumn("key3", lit(1024)) //把第三列全部改为1024
        df.withColumn("key3", $"key3" * 2) //把第三列*2，会把原来的第三列覆盖掉。
        df.withColumn("key5", expr("key3 * 2")) //添加列key5，值为key3的两倍

        //常用的是UDF方法：重点重点重点
        df.withColumn("key6", getXXXInfo(col("key3")))
        def getXXXInfo: UserDefinedFunction = {
            udf((key: Int) =>
                "这个数是：" + key
            )
        }

        df.withColumnRenamed("key3", "新名字")

        df.drop($"key2") //删除列
        df.drop(col("key2")) //删除列
        df.drop(df.schema.apply(2).name)


        // 计算数字和字符串列的基本统计信息，包括count，mean平均值，stddev标准差，min和max。
        // 如果没有给出列，则此函数计算所有数字或字符串列的统计信息。
        // 如果要以编程方式计算摘要统计信息，请使用`agg`函数。
        df.describe("key1", "key2")
        //2.2版本没有
        //df.summary().show()
        //df.summary("mean")

        //以数组形式返回所有列名
        df.columns //res83: Array[String] = Array(key1, key2, key3)
        df.schema //res85: org.apache.spark.sql.types.StructType = StructType(StructField(key1,StringType,true), StructField(key2,IntegerType,false), StructField(key3,IntegerType,false))
    }

    val 对Null的操作 = 0
    if (0) {
        //—————————————————————————对Null的操作—————————————————————————————————
        //将key2，3的null全部填充为xxx
        df.na.fill("xxx", Seq("key2", "key3"))
        //将key2中的null修改为666，将key3中的null值修改为"你好"
        df.na.fill(Map(("key2", 666), ("key3", "你好")))
        //过滤掉key3,key5列中含有null的行
        df.na.drop(Seq("key3", "key5"))
    }

    val 过滤操作 = 0
    if (0) {
        //———————————————————————————过滤操作———————————————————————
        //过滤操作
        df.filter("key2 > 2 and key3 = 5").show()
        df.filter((df("key2") > 2).and(df("key3") === 5)).show()
        df.filter((df("key2") > 2) and (df("key3") === 5)).show()
        df.filter((df("key2") > 2) || (df("key3") === 5)).show()

        val max = 4
        df.filter(s"key2 = ${max + 2}").select("key1").show()
        df.filter(df("key1") > "aaa").show()
        df.filter($"key2" === $"key3" - 1).show()

        //where等价于filter
        df.where($"key2" > "2")
        df.where("key2 > 2")

        //传递过滤函数,where没法传递
        df.filter(row => {
            //还能加一些条件
            val flag = row.getInt(1) > 2 //每一行的第二个元素大于2的行
            flag
        })
    }

    val 分组聚合操作 = 0
    if (0) {
        //对groupBy的聚合结果的计数
        df.groupBy("key1").count().show()
        df.groupBy($"key1").count().show()
        df.groupBy("key1", "key2") //org.apache.spark.sql.RelationalGroupedDataset

        df.groupBy("key1").count.withColumnRenamed("count", "cnt").sort($"cnt".desc).show

        //聚合函数
        //等价sql:  select key1, count(key1) num, max(key2), avg(key3) from table  group by key1
        df.groupBy("key1").agg(count("key1").as("num"), max("key2"), min("key2"), sum("key3"), avg("key3")).show
    }

    val 对聚合操作后的Value进行操作 = 0
    if (0) {
        //具体案例见sparkdemo.practice.Demo05
        df.groupByKey(row => row.getAs[String]("key1")).mapGroups((key1, group) => {
            var res = ""
            val key = key1
            var sum = 0
            for (i <- group) {
                //group是Iterator[Row],比如key=bbb的为[bbb,3,4][bbb,4,6]
                //对Key2进行求和并且*2运算
                sum = sum + i.getInt(1)
            }
            res = res + (sum * 2)
            (key, res) //默认为_1,_2
        }).show()
    }

    val 窗口函数 = 0
    //参考：
    //https://n3xtchen.github.io/n3xtchen/spark/2017/01/24/spark200-window-function
    //https://www.jianshu.com/p/42be8650509f
    if (0) {
        /*
        有些时候需要计算一些排序特征，窗口特征等，如一个店铺的首单特征。对于这样的特征显然是不能简单通过groupBy操作来完成
        即：第一列是订单，第二列是店铺，第三列是支付时间，第四列是价格。
        1、统计每个店铺每个订单和前一单的价格和，如果通过groupBy来完成特别费劲。
        2、店铺这个订单与前一单的差值 TODO 怎么做，难道要自定义聚合函数？
         */
        val orders = Seq(
            ("o1", "s1", "2017-05-01", 100),
            ("o2", "s1", "2017-05-02", 200),
            ("o3", "s2", "2017-05-01", 200),
            ("o4", "s1", "2017-05-03", 200),
            ("o5", "s2", "2017-05-02", 100),
            ("o6", "s1", "2017-05-04", 300)
        ).toDF("order_id", "seller_id", "pay_time", "price")

        //店铺订单顺序
        val rankSpec = Window.partitionBy("seller_id").orderBy("pay_time")
        orders.withColumn("rank", dense_rank.over(rankSpec)).show()
        //定义前一单和本单的窗口
        val winSpec = Window.partitionBy("seller_id").orderBy("pay_time").rowsBetween(-1, 0)
        //店铺这个订单及前一单的价格和
        orders.withColumn("sum_pay", sum("price").over(winSpec)).show()
        //店铺这个订单与前一单的差值,需要自定义聚合函数？？？
        orders.withColumn("minus_pay", avg("price").over(winSpec)).show()
    }

    val 排序 = 0
    if (0) {
        //orderby是调用的sort
        df.orderBy(df("key1").desc, df("key2").asc_nulls_first).limit(3).show()
        df.sort(df("key1").desc).show()
    }

    val 单表操作 = 0
    if (0) {
        df.show(10, 2) //参数1表示显示10行，参数2表示截取前两个字符
        //排序
        df.orderBy($"key1".desc).show()
        df.sort($"key1".desc).show()

        //获取若干行记录,返回Row类型
        df.first() //res50: org.apache.spark.sql.Row = [aaa,1,2]
        df.head() //同上
        df.head(3) //res52: Array[org.apache.spark.sql.Row] = Array([aaa,1,2], [bbb,3,4], [ccc,3,5])
        df.take(3) //同上
        df.takeAsList(2) //res55: java.util.List[org.apache.spark.sql.Row] = [[aaa,1,2], [bbb,3,4]]
        //limit方法获取指定DataFrame的前n行记录，得到一个新的DataFrame对象。和take与head不同的是，limit方法不是Action操作
        df.limit(2)

        //收集所有行到数组
        df.collect()
        df.collectAsList()

        //删除重复行行操作
        df.distinct()
        df.dropDuplicates() //删除重复行，distinct的别名
        //删除某一列具有重复的元素所在的行
        df.dropDuplicates("key1") //常用

        df.rdd.zipWithUniqueId()
        //Array[(org.apache.spark.sql.Row, Long)] = Array(([aaa,1,2],0), ([bbb,3,4],1), ([ccc,3,5],2), ([bbb,4,6],3))

    }

    val 多表操作 = 0
    if (0) {
        //两个DF的连接操作,通过主键连接
        val df1 = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")
        val df2 = spark.createDataset(Seq(("aaa", 2, 2), ("bbb", 3, 5), ("ddd", 3, 5), ("bbb", 4, 6), ("eee", 1, 2), ("aaa", 1, 5), ("fff", 5, 6))).toDF("key1", "key2", "key4")

        println("求并集：union====Cost：Low=========")
        df1.union(df2).show()
        println("求交集：intersect===Cost:Expensive=========")
        df1.intersect(df2).show()
        println("求差集：except===Cost:Expensive=========")
        df1.except(df2).show()

        //内连接,左右必须同时满足的才可以显示
        df1.join(df2, "key1").show() //推荐
        df1.join(df2, df1("key1") === df2("key1")).show() //多了一列key1,还要drop(df2.col("key1"))
        //等值连接（两个公共字段key1，key2）
        df1.join(df2, Seq("key1", "key2")).show()

        //还是内连接，这次用joinWith。和join的区别是连接后的新Dataset的schema会不一样
        df1.joinWith(df2, df1("key1") === df2("key1"))
        /*
        +---------+---------+
        |       _1|       _2|
        +---------+---------+
        |[aaa,1,2]|[aaa,1,5]|
        |[aaa,1,2]|[aaa,2,2]|
         */

        //左连接(左外连接),只显示左边的，右边的填充null
        df1.join(df2, df1("key1") === df2("key1"), "left").show()
        df1.join(df2, df1("key1") === df2("key1"), "left_outer").show()
        //左半连接，等价于select * from user where id in (select user_id from job);
        df1.join(df2, df1("key1") === df2("key1"), "leftsemi").show()
        //右连接，对应的就不写了。

        //外连接，左右不管是否满足都显示出来
        df1.join(df2, df1("key1") === df2("key1"), "outer")
        df1.join(df2, df1("key1") === df2("key1"), "full")

        //条件连接
        df1.join(df2, df1("key1") === df2("key1") && df1("key2") > df2("key2"))
    }

    val 保存DF的方式 = 0
    if (0) {
        //————————————————————————————保存DF的方式——————————————————————————————————
        //  保存操作可以选择使用SaveMode，它指定如何处理现有数据（如果存在）。
        // 重要的是要意识到这些保存模式不使用任何锁定并且不是原子的。此外，执行覆盖时，将在写出新数据之前删除数据。
        // SaveMode.ErrorIfExists//如果数据已存在，则会引发异常。
        // SaveMode.Append  //附加到现有数据。
        // SaveMode.Overwrite //覆盖数据
        // SaveMode.Ignore//如果数据已存在，则预期保存操作不会保存DataFrame的内容并且不会更改现有数据。

        //通用的保存模式：format保持文件的类型，mode保存模式，
        df.write.format("parquet").mode(SaveMode.Overwrite).save()
        df.write.mode(SaveMode.Overwrite).save("F:/桌面/API/Scala/SparkDemo1/src/source/hello.txt")

        //保存到持久表
        df.write.saveAsTable("df")

        df.write.json("F:/桌面/API/Scala/SparkDemo1/src/source/test.json")
        df.write.parquet("src/sparkdemo/testfile/xxx")
        df.write.parquet("hdfs://localhost:9000/user/wcinput/df")

        //分区，分桶以及排序
        //partitionBy 创建一个 directory structure （目录结构）
        df.write.partitionBy("key1").bucketBy(42, "name").saveAsTable("people_partitioned_bucketed")
        df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    }

    val Schema合并 = 0
    //没什么卵用
    if (0) {
        val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
        squaresDF.write.parquet("src/sparkdemo/testfile/test_table/key=1")

        val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
        cubesDF.write.parquet("src/sparkdemo/testfile/test_table/key=2")

        val mergedDF = spark.read.option("mergeSchema", "true").parquet("src/sparkdemo/testfile/test_table")
        mergedDF.printSchema()
    }

    val UDF = 0
    /*
    UDF和UDAF对DataFrame来说是非常重要的，否则处理复杂业务还需要转化为RDD，然后再处理，代价是昂贵的。
    如果你用Python的话，为避免性能损失，建议使用spark pandas。
     */
    if (0) {
        //方式1：不推荐(没有代码检查，容易出错)
        df.createOrReplaceTempView("testUDF")
        //UDF用户定义的函数，针对单行输入，返回一个输出
        spark.udf.register("product", (x: Integer, y: Integer) => (x * y, 11))
        //直接在SQL语句中使用UDF，就像使用SQL自动的内部函数一样
        spark.sql("select key1,product(key2,key3) as prodect from testUDF").show

        //方式2：推荐，如果udf和col是红色的，说明没有这个函数，其实需要导包
        import org.apache.spark.sql.functions._
        val udf1 = udf((input: String) =>
            input.length
        )
        //特别推荐，因为可以将方法抽出来。
        def getUdf: UserDefinedFunction = {
            udf((input: String) => input.length)
        }
        df.withColumn("key4", udf1($"key1"))
        df.withColumn("key4", getUdf($"key1"))
    }

    val 内置的聚合函数 = 0
    /*
    内置的聚合函数：
    avg：平均值
    first：分组第一个元素
    last：分组最后一个元素
    max：最大值
    min：最小值
    mean：平均值
    count：计数
    countDistinct：去重计数 SQL中用法   select count(distinct class)
    sum：求和
    sumDistinct：非重复值求和 SQL中用法    select sum(distinct class)
    approx_count_distinct：count_distinct近似值
    collect_list：聚合指定字段的值到list
    collect_set：聚合指定字段的值到set
    corr：计算两列的Pearson相关系数
    covar_pop：总体协方差（population covariance）
    covar_samp：样本协方差（sample covariance）
    grouping
    grouping_id
    kurtosis：计算峰态(kurtosis)值
    skewness：计算偏度(skewness)
    stddev：即stddev_samp
    stddev_samp：样本标准偏差（sample standard deviation）
    stddev_pop：总体标准偏差（population standard deviation）
    var_pop：总体方差（population variance）
    var_samp：样本无偏方差（unbiased variance）
    */


    val UDAF = 0
    /*
    UDAF用户定义的聚合函数。针对多行输入，返回一个输出。
    虽然特别复杂，但是比DF中的groupByKey——mapGroups函数更加高效。
    虽然UDAF可以用Java或者Scala实现，但是建议您使用Java，因为Scala的数据类型有时会造成不必要的性能损失。
    参考：https://help.aliyun.com/document_detail/69553.html
    具体案例见sparkdemo.practice.Demo05
    */
    if (1) {
        val df2 = df
            .withColumn("key2",intToLong($"key2"))
            .withColumn("key3",intToLong($"key3"))
        df2.show()

        df2.createOrReplaceTempView("testUDF")
        //方式1：SQL
        spark.udf.register("myAvg", new MyAverage) //参数2是个Object
        spark.sql("select key1, myAvg(key2) as avg from testUDF group by key1").show()

        //方式2：DF
        def getAvgUdaf:UserDefinedAggregateFunction = {
            new MyAverage
        }
        df2.groupBy("key1").agg(getAvgUdaf($"key2")).show()
        df2.withColumn("avg",getAvgUdaf($"key2")).show()

       /*
        import org.apache.spark.sql.functions._
        //类型安全的用户自定义函数。
        val ds = df.as[KeyDemo]
        val average_key2 =(new MyAverage2).toColumn.name("average_key2")
        val result = ds.select(average_key2)
        result.show()

       //类型安全的用户定义聚合函数;强类型数据集的用户定义聚合围绕Aggregator抽象类。例如，类型安全的用户定义平均值可能如下所示：
        case class KeyDemo(key1: String, key2: Integer, key3:Integer)
        case class Average(var sum: Long, var count: Long)

        class MyAverage2 extends Aggregator[KeyDemo, Average, Double] {
            //此聚合的零值。应该满足任何b + zero = b的属性
            def zero: Average = Average(0L, 0L)
            // 合并两个值以生成新值。为了提高性能，该函数可以修改`buffer`并返回它而不是构造一个新对象
            def reduce(buffer: Average, keyDemo: KeyDemo): Average = {
                buffer.sum += keyDemo.key2 //一旦有确定了属性，就说明不适合所有的DS
                buffer.count += 1
                buffer
            }
            //合并两个中间值
            def merge(b1: Average, b2: Average): Average = {
                b1.sum += b2.sum
                b1.count += b2.count
                b1
            }
            // 计算最终结果
            def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
            // 为中间值类型指定编码器
            def bufferEncoder: Encoder[Average] = Encoders.product
            // 为最终输出值类型指定编码器
            def outputEncoder: Encoder[Double] = Encoders.scalaDouble
        }*/
    }

    /**
      * 无类型用户定义的聚合函数,用户必须扩展UserDefinedAggregateFunction抽象类以实现自定义无类型聚合函数。
      * 而且必须定义为全局变量，放在if内部会报错：java.lang.InternalError: Malformed class name
      */
    class MyAverage extends UserDefinedAggregateFunction{
        //继承抽象函数必须实现以下方法
        // 输入参数的数据类型
        def inputSchema: StructType = StructType(StructField("value", LongType) :: Nil)
        // 缓冲区中进行聚合时，所处理的数据的类型
        def bufferSchema: StructType = StructType(StructField("count", LongType) :: StructField("sum", DoubleType) :: Nil)

        // 初始化给定的聚合缓冲区，即聚合缓冲区的零值。   请注意，缓冲区内的数组和映射仍然是不可变的。
        def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L  //表示次数
            buffer(1) = 0.0D  //表示总和
        }
        //使用来自input的新输入数据更新给定的聚合缓冲区`buffer`。每个输入行调用一次。
        def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            if (!input.isNullAt(0)) {
                buffer(0) = buffer.getLong(0) + 1L  //次数加1
                buffer(1) = buffer.getDouble(1) + input.getAs[Long](0).toDouble  //求和
            }
        }
        // 此函数是否始终在相同输入上返回相同的输出
        def deterministic: Boolean = true
        // 合并两个聚合缓冲区并将更新的缓冲区值存储回“buffer1”。当我们将两个部分聚合的数据合并在一起时调用此方法。
        // Spark是分布式的，所以不同的区需要进行合并。
        def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)  //求次数
            buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)  //求和
        }
        //计算最终的结果
        def evaluate(buffer: Row): Double = {
            buffer.getDouble(1) / buffer.getLong(0).toDouble
        }
        // 返回值的数据类型
        def dataType: DataType = DoubleType
    }

    val repartition的三个重载函数的区别 = 0
    //参考：http://www.cnblogs.com/lillcol/p/9885080.html
    // 涉及到关联操作的时候，对数据进行重新分区操作可以提高程序运行效率
    // 内部是通过shuffle进行操作的。
    // 很多时候效率的提升远远高于重新分区的消耗，所以进行重新分区还是很有价值的
    if (0) {
        //有明确的分区
        df.repartition(10)

        //它由保留现有分区数量的给定分区表达式划分。得到的DataFrame是哈希分区的。
        // 这与SQL (Hive QL)中的“distribution BY”操作相同
        //根据 key1 字段进行分区，分区数量由 spark.sql.shuffle.partition 决定 //默认200个
        df.repartition(col("key1"))

        // 由给定的分区表达式划分为 'numpartition' 。得到的DataFrame是哈希分区的。
        // 这与SQL (Hive QL)中的“distribution BY”操作相同。
        // 根据 key1 字段进行分区，将获得10个分区的DataFrame，此方法有时候在join的时候可以极大的提高效率，但是得注意出现数据倾斜的问题
        df.repartition(10, col("key1"))

        //减少分区数量，避免shuffle
        df.coalesce(3) //当df有10个分区减少到3个分区时，不会触发shuffle
        df.coalesce(100) //触发shuffle 返回一个100分区的DataFrame，等价于repartition(100)
    }


}
