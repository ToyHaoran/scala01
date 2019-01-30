package sparkdemo

import org.apache.spark.sql.expressions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.BaseUtil._
import utils.UdfUtil._
import utils.ConnectUtil

object DataFrameDemo extends App {

  //1.6后基本上不用sc了，用spark.sparkContext代替
  val sc = ConnectUtil.sc
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
    df.explain()
    df.explain(true)
    df.rdd.toDebugString

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
        .option("sep", ",") //csv中的切割符，默认逗号
        .option("inferSchema", "true") //是否自动推断类型，没有的话全部是String
        .option("header", "true") //是否将第一行作为Schema
        .load("file:/usr/local/jar/lihaoran/测试.csv") //文件路径
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
        .option("url", "jdbc:dialect:serverName")
        .option("user", "user")
        .option("password", "pass")
        .option("dbtable", "table").save()
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
    ds.select($"name".as[String], ($"age" > 10).as[Boolean]).toDF()
  }

  /*
   DataFrame
     大多数情况下的最佳选择
     通过 Catalyst 提供查询优化
     全程代码生成
     直接内存访问
     垃圾回收 (GC) 开销低
     不如 DataSet 那样适合开发，因为没有编译时检查或域对象编程

   DataSet
     适合不太影响性能的复杂 ETL 管道
     不适合性能影响可能很大的聚合
     通过 Catalyst 提供查询优化
     提供域对象编程和编译时检查，适合开发
     增加序列化/反序列化开销
     GC 开销高
     中断全程代码生成

   RDD
     在 Spark 2.x 中不必使用 RDD，除非需要生成新的自定义 RDD
     不能通过 Catalyst 提供查询优化
     不提供全程代码生成
     GC 开销高
     必须使用 Spark 1.x 旧版 API

   如何选择：
     如果您需要丰富的语义，高级抽象和域特定的API，请使用DataFrame或Dataset。
     如果您的处理需要高层次表达式 filters, maps, aggregation, averages, sum, SQL queries, columnar access和对半结构化数据使用lambda函数，请使用DataFrame或Dataset。
     如果要在编译时要求更高程度的类型安全性，则需要类型化的JVM对象，利用Catalyst优化，并从Tungsten的高效代码生成中受益，使用Dataset。
     如果要在Spark Libraries之间统一和简化API，请使用DataFrame或Dataset。
     如果你是一个R用户，使用DataFrame。
     如果你是一个Python用户，使用DataFrame

   以下情况选择RDD：
     如果你需要更多的控制功能，尽量回到RDD。
     数据是非结构化的时候，例如 ：media streams or streams of text (流媒体或者文本流)
     使用函数式编程来操作你的数据，而不是用特定领域语言(DSL)表达
     放弃针对结构化和半结构化数据的DataFrames和Datasets提供的一些优化和性能时。

   总之：
     RDD提供低级别的功能和更多的控制。
     DF/DS允许自定义视图和结构，提供高级和特定领域的操作，节省空间，并能以极高的速度运行。
    */
  val 三者的优缺点 = 0
  if (0) {

  }

  /*
  背景：每次你对一个RDD执行一个算子操作时，都会重新从源头处计算一遍，计算出那个RDD来，然后再对这个RDD执行你的算子操作。
  解决：加缓存。
  注意：cache之后并不会引发计算，必须count(计算一次)之后，在Storage中才会出现缓存

  如何选择：
    1、默认情况下，性能最高的当然是MEMORY_ONLY，但前提是你的内存必须足够足够大，可以绰绰有余地存放下整个RDD的所有数据。
      因为不进行序列化与反序列化操作，就避免了这部分的性能开销。
      如果RDD中数据比较多时（比如几十亿），直接用这种持久化级别，会导致JVM的OOM内存溢出异常。
    2、如果使用MEMORY_ONLY级别时发生了内存溢出，那么建议尝试使用MEMORY_ONLY_SER级别。
      该级别会将RDD数据序列化后再保存在内存中，此时每个partition仅仅是一个字节数组而已，大大减少了对象数量，并降低了内存占用。
      这种级别比MEMORY_ONLY多出来的性能开销，主要就是序列化与反序列化的开销。
    3、如果纯内存的级别都无法使用，那么建议使用MEMORY_AND_DISK_SER策略，而不是MEMORY_AND_DISK策略。
      因为既然到了这一步，就说明RDD的数据量很大，内存无法完全放下。序列化后的数据比较少，可以节省内存和磁盘的空间开销。
      同时该策略会优先尽量尝试将数据缓存在内存中，内存缓存不下才会写入磁盘。
    4、通常不建议使用DISK_ONLY和后缀为_2的级别：
      因为完全基于磁盘文件进行数据的读写，会导致性能急剧降低，有时还不如重新计算一次所有RDD。
      后缀为_2的级别，必须将所有数据都复制一份副本，并发送到其他节点上，数据复制以及网络传输会导致较大的性能开销，
      除非是要求作业的高可用性，否则不建议使用。
   */
  val DF数据持久化 = 0
  if (0) {
    df.cache() //调用下面方法
    df.persist() //默认MEMORY_AND_DISK
    df.count() //必须计算一次才有缓存
    //如果要写入两个parquet，必须加缓存，否则会重新计算两次。
    //比如说费控数据初始化，不加缓存，写两个路径，又TM重新读取一次。

    //持久化级别
    import org.apache.spark.storage.StorageLevel
    df.persist(StorageLevel.DISK_ONLY)
    df.persist(StorageLevel.MEMORY_ONLY)

    //取消缓存，这个一般不需要，JVM自动垃圾回收。
    df.unpersist()
  }

  val SQL风格语法 = 0
  if (0) {
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
    val strdd = "upper(key1),key2 AS haha2"
    val arr05 = Array("aaa", "bbb")
    arr05.mkString("sss", ",", "dddd")
    df.selectExpr(strdd.split(","): _*).show()
    df.selectExpr("upper(key1)", "key2 as haha2").show
    df.select(expr("upper(key1)"))
    df.select(expr("key2 + 1").as[Int])

    //添加，修改，删除列操作（select中用when函数也可以，但是功能较少，被UDF取代）
    df.withColumn("key4", lit(1024)) //添加列key5，初始值为1024
    df.withColumn("key3", lit(1024)) //把第三列全部改为1024
    df.withColumn("key3", $"key3" * 2) //把第三列*2，会把原来的第三列覆盖掉。
    df.withColumn("key5", expr("key3 * 2")) //添加列key5，值为key3的两倍

    //更改列类型
    df.withColumn("key3", col("key3").cast(DoubleType))
    df.withColumn("key3", col("key3").cast("string"))

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

  val 一列分割为多行_一列分割为多列 = 0
  if (0) {
    val df = spark.createDataset(Seq(("aaa", "你好 我来自山东 你呢"), ("bbb", "嗯嗯 我来自四川"))).toDF("name", "message")
    /*
      一列分割多行：在name不动的情况下，拆分message为多行
        |name|message|
        | aaa|     你好|
        | aaa|  我来自山东|
        | aaa|     你呢|
        | bbb|     嗯嗯|
        | bbb|  我来自四川|
     */
    //方式1：组合法：适合少量字段（麻烦）。 逆操作使用聚合就行。
    df.flatMap(item => {
      item.getString(1).split(" ").map(msg => (item.getString(0), msg))
    }).toDF(df.columns: _*).show()
    //方式2：使用高级函数explode切割数组。推荐。
    df.withColumn("message", explode(split(col("message"), " "))).show()

    /*
    一列分割为多列：在name不动的情况下，拆分message为多列
      |name|msg1| msg2|
      | aaa|  你好|我来自山东|
      | bbb|  嗯嗯|我来自四川|
     */
    df.withColumn("message", split(col("message"), " "))
        //注意这里的getItem在split情况下是0 1，但是在流计算-窗口函数-里面的timestamp里面确是start，end，所以最好打印一下schema看一下
        .select($"name", $"message".getItem(0).as("msg1"), $"message".getItem(1).as("msg2"))
        .show()
    //对ArrayType进行处理,一种方式是处理成两列，另一种方式是直接操作(放弃，没什么用)

    /*
    将多列合并为一列：将name和message合并。
      |          value|
      |aaa,你好 我来自山东 你呢|
      |   bbb,嗯嗯 我来自四川|
     */
    //方式1：使用UDF
    //方式2：使用内置函数concat_ws
    df.select(concat_ws(",", $"name", $"message").as("value").cast("string")).show()
  }

  val 对行的操作 = 0
  if (0) {
    //取前几行
    val top5: DataFrame = df.limit(5) //取前5行
    val top4: Array[Row] = df.head(4)
    val sample = df.take(4)
  }
  val 多列分割为多行 = 0
  //见费控表码开放

  val 对Null的操作 = 0
  if (0) {
    //对某个键，比如说GDDWBM，想要找出来所有表中都有的GDDWBM，需要一个一个取交集。
    //怎么判断一个df是否为null
    var df007: DataFrame = null
    //不要df007.head(1).isEmpty，使用.head的时候就报错:空指向异常。
    if (null == df007) {
      df007 = df.distinct()
    } else {
      df007 = df007.intersect(df.distinct())
    }

    //将key2，3的null全部填充为xxx
    df.na.fill("xxx", Seq("key2", "key3"))
    //将key2中的null修改为666，将key3中的null值修改为"你好"
    df.na.fill(Map(("key2", 666), ("key3", "你好")))
    //过滤掉key3,key5列中含有null的行
    df.na.drop(Seq("key3", "key5"))

    //注意：下面这个会导致key1全部为null,必须使用UDF
    df.withColumn("key1", col("key1") + "A").show()
  }

  val 过滤操作 = 0
  if (0) {
    //过滤操作
    df.filter("key2 > 2 and key3 = 5").show()
    df.filter("key2 in (1,2,3)").show()
    df.filter((df("key2") > 2).and(df("key3") === 5)).show()
    df.filter((df("key2") > 2) and (df("key3") === 5)).show()
    df.filter((df("key2") > 2) || (df("key3") === 5)).show()

    val max = 4
    df.filter(s"key2 = ${max + 2}").select("key1").show()
    df.filter(df("key1") > "aaa").show()
    df.filter($"key2" === length($"key3") - 1).show()

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
  /*
  参考：
  https://n3xtchen.github.io/n3xtchen/spark/2017/01/24/spark200-window-function
  https://www.jianshu.com/p/42be8650509f
   */
  if (0) {
    /*
    有些时候需要计算一些排序特征，窗口特征等，如一个店铺的首单特征。对于这样的特征显然是不能简单通过groupBy操作来完成
    即：第一列是订单，第二列是店铺，第三列是支付时间，第四列是价格。
    1、统计每个店铺每个订单和前一单的价格和，如果通过groupBy来完成特别费劲。
    2、店铺这个订单与前一单的差值，需要自定义聚合函数
    还有计算前4秒的平均值、计算环比之类的，都要用到窗口函数。
     */
    val orders = Seq(
      ("o1", "s1", "2017-05-01", 100),
      ("o2", "s1", "2017-05-02", 200),
      ("o3", "s2", "2017-05-01", 200),
      ("o4", "s1", "2017-05-03", 200),
      ("o5", "s2", "2017-05-02", 100),
      ("o6", "s1", "2017-05-04", 300)
    ).toDF("order_id", "seller_id", "pay_time", "price")
    //打印分区信息
    orders.printItemLoc()

    //店铺订单顺序
    val rankSpec = Window.partitionBy("seller_id").orderBy("pay_time")
    orders.withColumn("rank", dense_rank.over(rankSpec)).show()
    val rankSpec2 = Window.partitionBy("seller_id").orderBy("price")
    orders.withColumn("rank2", rank.over(rankSpec2)).show() //1,2,2,4
    orders.withColumn("dense_rank2", dense_rank.over(rankSpec2)).show() //1,2,2,3

    //定义前一单和本单的窗口
    val winSpec = Window.partitionBy("seller_id").orderBy("pay_time").rowsBetween(-1, 0)
    //店铺这个订单及前一单的价格和
    orders.withColumn("sum_pay", sum("price").over(winSpec)).show()
    //店铺这个订单与前一单的平均值，用UDAF
    def getAvgUdaf: UserDefinedAggregateFunction = new MyAverage
    orders.withColumn("avg", getAvgUdaf($"price").over(winSpec)).show()
    orders.withColumn("avg2", avg("price").over(winSpec)).show()

    //每个店铺当前订单与前一单的差值,需要自定义聚合函数，或者lag函数
    def getMinusUdaf: UserDefinedAggregateFunction = new MyMinus
    orders.withColumn("rank", dense_rank.over(rankSpec))
        .withColumn("prePrice", lag("price", 1).over(rankSpec)) //前一行的值
        .withColumn("minus", getMinusUdaf($"price").over(winSpec)) //在前面的基础上用UDF也行
        .show()

    /*
    lag(field, n): 就是取从当前字段往前第n个值，这里是取前一行的值
    first/last(): 提取这个分组特定排序的第一个最后一个，在获取用户退出的时候，你可能会用到
    lag/lead(field, n): lead 就是 lag 相反的操作，这个用于做数据回测特别用，结果回推条件
     */
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

    df.selectExpr("max(key2)").show()


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

  val 多表合并 = 0
  if (0) {
    //两个DF的连接操作,通过主键连接
    //val df1 = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")
    //val df2 = spark.createDataset(Seq(("aaa", 2, 2), ("bbb", 3, 5), ("ddd", 3, 5), ("bbb", 4, 6), ("eee", 1, 2), ("aaa", 1, 5), ("fff", 5, 6))).toDF("key1", "key2", "key4")
    val df1 = spark.createDataset(Seq(("aaa", 1, 2))).toDF("key1", "key2", "key3")
    val df2 = spark.createDataset(Seq(("aaa", 2, 2), ("bbb", 3, 5))).toDF("key3", "key2", "key1")

    println("求并集：union====Cost：Low=========")
    df1.union(df2).show() //注意Union字段必须对应，否则会出错。
    println("求交集：intersect===Cost:Expensive=========")
    df1.intersect(df2).show()
    println("求差集：except===Cost:Expensive=========")
    df1.except(df2).show()
  }

  val 多表Join = 0
  if (0) {
    val df1 = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6))).toDF("key1", "key2", "key3")
    val df2 = spark.createDataset(Seq(("aaa", 2, 2), ("bbb", 3, 5), ("ddd", 3, 5), ("bbb", 4, 6), ("eee", 1, 2), ("aaa", 1, 5), ("fff", 5, 6))).toDF("key1", "key2", "key4")

    //DataFrame内连接,左右必须同时满足的才可以显示
    df1.join(df2, "key1").show() //推荐
    df1.join(df2, df1("key1") === df2("key1"), "fullouter").show() //多了一列key1,还要drop(df2.col("key1"))
    //等值连接（两个公共字段key1，key2）
    df1.join(df2, Seq("key1", "key2")).show()
    //自连接
    df1.as("a").join(df1.as("b")).where($"a.key1" === $"b.key1").show()
    //DataSet内连接，这次用joinWith。和join的区别是连接后的新Dataset的schema会不一样
    df1.joinWith(df2, df1("key1") === df2("key1")).show()

    df1.withColumnRenamed("key3", "key4")
    /*
    |       _1|       _2|
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

    /*
    如果df2比df1小的多，将小表广播到其他节点，不走shuffle过程。
    设置自动广播：config("spark.sql.autoBroadcastJoinThreshold", "209715200")
    会自动广播小于10M的表，broadcast表的最大值10M（10485760），当为-1时，broadcasting不可用，内存允许的情况下加大这个值
     */
    df1.join(broadcast(df2), "key1").show()

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

    df.write.csv("src/sparkdemo/testfile/xxxxx")
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
  val 内置的聚合函数 = 0

  /*
  UDAF用户定义的聚合函数。针对多行输入，返回一个输出。
  虽然特别复杂，但是比DF中的groupByKey——mapGroups函数更加高效。
  虽然UDAF可以用Java或者Scala实现，但是建议您使用Java，因为Scala的数据类型有时会造成不必要的性能损失。
  参考：https://help.aliyun.com/document_detail/69553.html
  具体案例见sparkdemo.practice.Demo05
  */
  val UDAF = 0
  if (0) {
    val df2 = df
        .withColumn("key2", intToLong($"key2"))
        .withColumn("key3", intToLong($"key3"))
    df2.show()

    df2.createOrReplaceTempView("testUDF")
    //方式1：SQL
    spark.udf.register("myAvg", new MyAverage) //参数2是个Object
    spark.sql("select key1, myAvg(key2) as avg from testUDF group by key1").show()

    //方式2：DF
    def getAvgUdaf: UserDefinedAggregateFunction = {
      new MyAverage
    }
    df2.groupBy("key1").agg(getAvgUdaf($"key2").as("avg")).show()
    //df2.withColumn("avg",getAvgUdaf($"key2")).show()  //报错

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

  class MyAverage extends UserDefinedAggregateFunction {
    //继承抽象函数必须实现以下方法
    // 输入参数的数据类型
    def inputSchema: StructType = StructType(StructField("value", LongType) :: Nil)

    // 缓冲区中进行聚合时，所处理的数据的类型
    def bufferSchema: StructType = StructType(StructField("count", LongType) :: StructField("sum", DoubleType) :: Nil)

    // 初始化给定的聚合缓冲区，即聚合缓冲区的零值。   请注意，缓冲区内的数组和映射仍然是不可变的。
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L //表示次数
      buffer(1) = 0.0D //表示总和
    }

    //使用来自input的新输入数据更新给定的聚合缓冲区`buffer`。每个输入行调用一次。
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + 1L //次数加1
        buffer(1) = buffer.getDouble(1) + input.getAs[Long](0).toDouble //求和
      }
    }

    // 此函数是否始终在相同输入上返回相同的输出
    def deterministic: Boolean = true

    // 合并两个聚合缓冲区并将更新的缓冲区值存储回“buffer1”。当我们将两个部分聚合的数据合并在一起时调用此方法。
    // Spark是分布式的，所以不同的区需要进行合并。
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0) //求次数
      buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1) //求和
    }

    //计算最终的结果
    def evaluate(buffer: Row): Double = {
      buffer.getDouble(1) / buffer.getLong(0).toDouble
    }

    // 返回值的数据类型
    def dataType: DataType = DoubleType
  }

  class MyMinus extends UserDefinedAggregateFunction {
    def inputSchema: StructType = StructType(StructField("value", LongType) :: Nil)

    def bufferSchema: StructType = StructType(StructField("minus", LongType) :: Nil)

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L //表示差值
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        //输入的后者减去前者
        buffer(0) = input.getLong(0) - buffer.getLong(0)
      }
    }

    // 此函数是否始终在相同输入上返回相同的输出
    def deterministic: Boolean = true

    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //分区合并，也是后者减前者
      buffer1(0) = buffer2.getLong(0) - buffer1.getLong(0)
    }

    //计算最终的结果
    def evaluate(buffer: Row): Long = {
      buffer.getLong(0)
    }

    // 返回值的数据类型
    def dataType: DataType = LongType
  }

  val repartition的三个重载函数的区别 = 0
  //参考：http://www.cnblogs.com/lillcol/p/9885080.html
  // 涉及到关联操作的时候，对数据进行重新分区操作可以提高程序运行效率
  // 内部是通过shuffle进行操作的。
  // 很多时候效率的提升远远高于重新分区的消耗，所以进行重新分区还是很有价值的
  if (0) {
    //有明确的分区
    df.repartition(10)
    df.rdd.getNumPartitions

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
