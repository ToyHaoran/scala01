package sparkdemo

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import utils.BaseUtil.int2boolen
import utils.ConnectUtil

object DataFrameDemo {

    val sc = ConnectUtil.getLocalSC  //基本上后来不用sc了，用spark.sparkContext代替

    val spark = ConnectUtil.getLocalSpark

    import spark.implicits._

    val df = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")

    case class Person(name: String, age: Int)

    //val people = sc.textFile("src/sparkdemo/testfile/person.txt")
    val people = sc.textFile("file:/usr/local/jar/lihaoran/person.txt")
        .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF() //转化为df后，统统为Row类型

    val DF打印对齐 = 0
    if (0) {
        df.show(30) //最多打印30行
        df.show(30, truncate = 20) // 每个元素最大20个字符，其他折叠
        df.show(30, truncate = false) // 每个元素不折叠
        //注意shell中极有可能head对不齐，但是在Spark web UI中可以对齐
    }

    val 创建DF的方式 = 0
    if (0) {
        //需要先导入隐式操作(否则toDF等快捷函数不能用)
        import spark.implicits._
        //方法一，Spark中使用toDF函数创建DataFrame，如果不指定列名字，默认为"_1","_2"..........
        val df = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")
        val df_1 = Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6)).toDF("key1", "key2", "key3") //同上
        //通过case class + toDF创建DataFrame的示例：开头的people的创建

        //方法二，Spark中使用createDataFrame函数创建DataFrame
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

        //方法三，通过文件直接创建DataFrame(推荐)
        val df4 = spark.read.parquet("hdfs:/path/to/file") //使用parquet文件创建
        val df4_1 = spark.read.parquet("E:/hivedata1/xxxx") //使用parquet文件创建
        val df5 = spark.read.json("examples/src/main/resources/people.json") //使用json文件创建
        val df6 = spark.read.format("csv") //使用csv文件,spark2.0+之后的版本可用
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("file:/usr/local/jar/lihaoran/测试.csv").show()
        //.load("sparkdemo/testfile/测试.csv")
        //直接在文件上运行SQL
        val df7 = spark.sql("SELECT * FROM parquet.'examples/src/main/resources/users.parquet'")

        //从Hive中读取
        //Use SparkSession.builder.enableHiveSupport instead
        import org.apache.spark.sql.hive.HiveContext
        val hiveCtx = new HiveContext(sc)
        val rows = hiveCtx.sql("SELECT key, value FROM mytable")
        val keys = rows.map(row => row.getInt(0))

        //通过JDBC加载数据并创建df
        val url: String = "jdbc:oracle:thin:@172.20.32.97:1521:dyjc"
        val user = "RT_DFJS"
        val password = "ffffff"
        val dataBaseProps = new java.util.Properties()
        dataBaseProps.setProperty("user", user)
        dataBaseProps.setProperty("password", password)
        dataBaseProps.setProperty("fetchsize", "1000") //批量读
        dataBaseProps.setProperty("batchsize", "5000") //批量写
        val b_jcy_voltrank = spark.read.jdbc(url, "(select * from lc_cbxx where rownum<=1000)", dataBaseProps) //write是一样的格式
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

        //添加，修改，删除列操作，
        df.withColumn("key4", lit(1024)) //添加列key5，初始值为1024
        df.withColumn("key3", lit(1024)) //把第三列全部改为1024
        df.withColumn("key3", $"key3" * 2) //把第三列*2，会把原来的第三列覆盖掉。
        df.withColumn("key5", expr("key3*2")) //添加列key5，值为key3的两倍

        //常用的是UDF方法：重点重点重点
        df.withColumn("key6", getXXXInfo(col("key3")))
        def getXXXInfo:UserDefinedFunction = {
            udf((key:Int) =>
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
        df.summary().show()
        df.summary("mean")

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
        df.filter("key2 > 2")
        val max = 4 //s表示简单的字符串插值器
        df.filter(s"key2 = ${max + 2}").select("key1")

        df.filter($"key1" > "aaa")
        df.filter($"key2" === $"key3" - 1)
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
        //———————————————————————————分组聚合操作————————————————————————

        //等价SQL: select key1, count(*) from table
        df.groupBy("key1").count() //对groupBy的聚合结果的计数
        df.groupBy("key1", "key2") //org.apache.spark.sql.RelationalGroupedDataset
        df.groupBy($"key1") //会出错，不知道为什么。。。

        //等价SQL: select distinct key1 from table
        df.select("key1").distinct.show

        //等价sql:  select key1 , count(*) from table  group by key1  order by count(*) desc
        df.groupBy("key1").count.sort($"count".desc).show

        //用withColumnRenamed函数给列重命名
        //等价sql: select key1 , count(*) as cnt from table group by key1 order by cnt desc
        df.groupBy("key1").count.withColumnRenamed("count", "cnt").sort($"cnt".desc).show

        //聚合函数
        //等价sql:  select key1, count(key1) num, max(key2), avg(key3) from table  group by key1
        df.groupBy("key1").agg(count("key1").as("num"), max("key2"), min("key2"), sum("key3"), avg("key3")).show
    }

    val 对聚合操作后的Value进行操作 = 0
    if (0) {
        //—————————————高级分组聚合———————————————————————————————
        //具体案例见例题4
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

    val 单表操作 = 0
    if (0) {
        //—————————————————————————单表操作———————————————————————————————

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

        //修改行操作
        df.distinct()
        df.dropDuplicates() //删除重复行，distinct的别名
        //删除某一列具有重复的元素所在的行
        df.dropDuplicates("key1") //常用
    }

    val 多表操作 = 0
    if (0) {
        //——————————————————————————多表操作—————————————————————————————————
        //两个DF的连接操作,通过主键连接
        val df1 = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")
        val df2 = spark.createDataset(Seq(("aaa", 2, 2), ("bbb", 3, 5), ("ddd", 3, 5), ("bbb", 4, 6), ("eee", 1, 2), ("aaa", 1, 5), ("fff", 5, 6))).toDF("key1", "key2", "key4")

        //内连接,左右必须同时满足的才可以显示
        df1.join(df2, "key1") //如下所示
        df1.join(df2, df1("key1") === df2("key1")) //多了一列key1
        //等值连接（两个公共字段key1，key2）
        df1.join(df2, Seq("key1", "key2"))

        //  还是内连接，这次用joinWith。和join的区别是连接后的新Dataset的schema会不一样
        df1.joinWith(df2, df1("key1") === df2("key1"))

        //左连接(左外连接),只显示左边的，右边的填充null
        df1.join(df2, df1("key1") === df2("key1"), "left").show()
        df1.join(df2, df1("key1") === df2("key1"), "left_outer").show()
        //左半连接，等价于select * from user where id in (select user_id from job);
        df1.join(df2, df1("key1") === df2("key1"), "leftsemi").show()
        //右连接，对应的就不写了。

        //  外连接，左右不管是否满足都显示出来
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


        df.write.format("parquet").mode(SaveMode.Overwrite).save()
        df.write.mode(SaveMode.Overwrite).save("F:\\桌面\\API\\Scala\\SparkDemo1\\src\\source\\hello.txt")


        df.write.saveAsTable("df")
        df.write.json("F:\\桌面\\API\\Scala\\SparkDemo1\\src\\source\\test.json")
        df.write.parquet("F:\\桌面\\API\\Scala\\SparkDemo1\\src\\source\\parquet")
        //保存到HDFS
        df.write.parquet("hdfs://localhost:9000/user/wcinput/df")
        //从HDFS读取文件
        val df11 = spark.read.parquet("hdfs://localhost:9000/user/wcinput/df")
    }

    /*val UDF和UDAF = 0
    if(0){
        //—————————————————————————UDF操作———————————————————————————————
        df.createOrReplaceTempView("testUDF")
        //UDF用户定义的函数，针对单行输入，返回一个输出
        spark.udf.register("product",(x:Integer,y:Integer)=>(x*y,11))
        //直接在SQL语句中使用UDF，就像使用SQL自动的内部函数一样
        spark.sql("select key1,product(key2,key3) as prodect from testUDF").show

        //推荐UDF使用方式，如果udf和col是红色的，说明没有这个函数，其实需要导包
        import org.apache.spark.sql.functions._
        val udf1 = udf((input:String)=>input.length)
        df.withColumn("key4",udf1($"key1"))



        //UDAF用户定义的聚合函数,在下面。针对多行输入，返回一个输出。
        //具体案例见培训例题4,推荐使用DF中的groupByKey，以及mapGroups函数
        //如果是object，可以直接写spark.udf.register("wordCount", MyAverage)
        spark.udf.register("myAvg", new MyAverage)
        spark.sql("select key1, myAvg(key2) as avg from testUDF group by key1").show()

        //类型安全的用户自定义函数。
        val ds = df.as[KeyDemo]
        val average_key2 =(new MyAverage2).toColumn.name("average_key2")
        val result = ds.select(average_key2)
        result.show()


        //无类型用户定义的聚合函数,用户必须扩展UserDefinedAggregateFunction抽象类以实现自定义无类型聚合函数。
        class MyAverage extends UserDefinedAggregateFunction{

            //继承抽象函数必须实现以下方法

            //输入参数的数据类型
            def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
            // 缓冲区中进行聚合时，所处理的数据的类型
            def bufferSchema: StructType = {StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)}
            // 返回值的数据类型
            def dataType: DataType = DoubleType

            // 初始化给定的聚合缓冲区，即聚合缓冲区的零值。
            // 请注意，缓冲区内的数组和映射仍然是不可变的。
            def initialize(buffer: MutableAggregationBuffer): Unit = {
                buffer(0) = 0L
                buffer(1) = 0L
            }

            //使用来自`input`的新输入数据更新给定的聚合缓冲区`buffer`。每个输入行调用一次。
            def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
                if (!input.isNullAt(0)) {
                    buffer(0) = buffer.getLong(0) + input.getLong(0)
                    buffer(1) = buffer.getLong(1) + 1
                }
            }

            // 此函数是否始终在相同输入上返回相同的输出
            def deterministic: Boolean = true

            // Spark是分布式的，所以不同的区需要进行合并。
            // 合并两个聚合缓冲区并将更新的缓冲区值存储回“buffer1”。当我们将两个部分聚合的数据合并在一起时调用此方法。
            def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
                buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
                buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
            }

            //计算最终的结果
            def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
        }

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
        }
    }*/


}
