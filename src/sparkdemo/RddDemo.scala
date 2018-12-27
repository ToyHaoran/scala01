package sparkdemo

import org.apache.spark.Partitioner
import utils.BaseUtil._
import utils.ConnectUtil

// RDD就是分布式的元素集合（弹性分布式数据集）
// 在spark中，对数据的所有操作不外乎创建

object RddDemo extends App {

  val sc = ConnectUtil.sc
  val spark = ConnectUtil.spark


  val RDD介绍 = 0
  /*
  代表一个惰性求值、不可变、可分区、里面的元素可并行计算的集合。

  在群集安装中，单独的数据分区可以位于不同的节点上。使用RDD作为句柄，可以访问所有分区并使用包含的数据执行计算和转换。每当RDD
  的一部分或整个RDD丢失时，系统就能够通过使用Lineage（谱系）信息来重建丢失分区的数据。谱系是指用于产生当前RDD的转换序列。
  因此，Spark能够从大多数故障中自动恢复。

  目前，spark中有四种可用的RDD API扩展：
  （在数据项满足上述要求时自动为用户提供具有扩展功能的方法）
  Double RDD Functions       此扩展包含许多用于聚合数值的有用方法。如果RDD的数据项可隐式转换为Scala数据类型double，则它们可用。
  Ordered RDD Functions      如果数据项是双组件元组，其中键可以隐式排序，则此接口扩展中定义的方法变为可用。
  PairRDDFunctions           当数据项具有双组件元组结构时，此接口扩展中定义的方法变为可用。
                             Spark将第一元组项（即tuplename.1）解释为关键字，将第二项（即tuplename.2）解释为关联值。
  SequenceFileRDDFunctions   此扩展包含几种允许用户从RDD创建Hadoop序列的方法。
                             数据项必须是PairRDDFunctions要求的两个组件键值元组。但是，考虑到元组组件可转换为可写类型，还有其他要求。

  RDD的属性
  1、一组分片（Partition），即数据集的基本组成单位。
      对于RDD来说，每个分片都会被一个计算任务处理，并决定并行计算的粒度。
      用户可以在创建RDD时指定RDD的分片个数，如果没有指定，那么就会采用默认值。
      默认值就是程序所分配到的CPU Core的数目。
  2、一个计算每个分区的函数。
      Spark中RDD的计算是以分片为单位的，每个RDD都会实现compute函数以达到这个目的。
      compute函数会对迭代器进行复合，不需要保存每次计算的结果。
  3、RDD之间的依赖关系。
      RDD的每次转换都会生成一个新的RDD，所以RDD之间就会形成类似于流水线一样的前后依赖关系。
      在部分分区数据丢失时，Spark可以通过这个依赖关系重新计算丢失的分区数据，而不是对RDD的所有分区进行重新计算。
  4、一个Partitioner，即RDD的分片函数。
      当前Spark中实现了两种类型的分片函数，一个是基于哈希的HashPartitioner，另外一个是基于范围的RangePartitioner。
      只有对于key-value的RDD，才会有Partitioner，非key-value的RDD的Parititioner的值是None。
      Partitioner函数不但决定了RDD本身的分片数量，也决定了parent RDD Shuffle输出时的分片数量。
  5、一个列表，存储存取每个Partition的优先位置（preferred location）。
      对于一个HDFS文件来说，这个列表保存的就是每个Partition所在的块的位置。
      按照“移动数据不如移动计算”的理念，Spark在进行任务调度的时候，会尽可能地将计算任务分配到其所要处理数据块的存储位置。
   */

  // 参考：http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html
  val 创建RDD的两种方式 = 0
  if (0) {
    println("Scala中的parallelize()方法============")
    val lines = sc.parallelize(List("hello  scala", "hello java", "hello scala"))
    val count = lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2).collect

    println("从文件中读取======")
    val lines2 = sc.textFile("src/sparkdemo/xxxxxx.md")
    lines2.foreach(println(_))
  }

  val 转化操作 = 0
  // 需要collect才能出结果的，
  // 一般是这种格式：res54: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[63] at mapPartitionsWithIndex at <console>:29

  val 行动操作 = 0
  // 可以直接出结果的，比如collect，reduce

  val map_mapValues_mapPartitions_mapPartitionsWithIndex = 0
  if (0) {
    println("map：对RDD的每个元素进行处理，并将结果作为新RDD返回=========")
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4))
    rdd1.map(_ + 1)
    rdd1.map(x => x + 1).collect() // Array[Int] = Array(2, 3, 4, 5)

    println("mapValues:对pair RDD中的每个value应用一个函数而不改变key==========")
    val rdd2 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    rdd2.mapValues(x => x + 1).collect //res41: Array[(Int, Int)] = Array((1,3), (3,5), (3,7))

    println("mapPartitions=========")
    // 输入一个函数对每个分区（每个分区只调用一次）进行处理，返回新的RDD
    // 各个分区的整个内容可通过输入参数（Iterarator [T]）作为连续的值流获得。
    // 自定义函数必须返回另一个迭代器[U]。组合的结果迭代器会自动转换为新的RDD。

    // 案例1：由于我们选择的分区，以下结果中缺少元组（3,4）和（6,7）
    val rdd3 = sc.parallelize(1 to 9, 3) //分区1：123；分区2：456；分区3：789
    def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
      var res = List[(T, T)]() //Nil
      var pre = iter.next //第一次迭代分区1：123；pre为1
      while (iter.hasNext) {
        val cur = iter.next; //第一次：2    第二次：3
        //注意.::等价于::表示向头部添加元素
        res.::=(pre, cur) //第一次：List(1,2)::Nil   第二次：List(2,3)
        pre = cur; //第一次：2       第二次：3(跳出循环)
      }
      res.iterator //第一个分区：List((2,3),(1,2))
    }
    rdd3.mapPartitions(myfunc).collect
    //Array[(Int, Int)] = Array((2,3), (1,2), (5,6), (4,5), (8,9), (7,8))

    //案例2：
    val rdd4 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
    def myfunc2(iter: Iterator[Int]): Iterator[Int] = {
      var res = List[Int]()
      while (iter.hasNext) {
        val cur = iter.next
        //创建一个新List，第一个参数是长度，第二个参数是值
        //res = res ::: List.fill(scala.util.Random.nextInt(10))(cur)
        //部分号码根本没有输出。这是因为为其生成的随机数为零。
        //Array[Int] = Array(1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 5, 7, 7, 7, 9, 9, 10)

        //每个数字是几就输出几次
        res = res ::: List.fill(cur)(cur)
        //Array[Int] = Array(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5,
        // 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8  8, 8,
        // 9, 9, 9, 9, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10)
      }
      res.iterator
    }
    rdd4.mapPartitions(myfunc2).collect

    //案例2的简化版：
    val rdd5 = sc.parallelize(1 to 10, 3)
    rdd5.flatMap(List.fill(scala.util.Random.nextInt(10))(_)).collect


    println("mapPartitionsWithIndex=============")
    // 与mapPartitions类似，传入的也是一个函数，但需要两个参数。
    // 第一个参数是分区的索引，第二个参数是该分区中所有项的迭代器。
    // 输出是一个迭代器，它包含应用函数编码的任何转换后的项列表。
    val rdd6 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
    rdd6.printItemLoc()
  }

  val flatMap_flatMapValues = 0
  if (0) {
    println("flatMap()=========")
    // 将函数应用于 RDD 中的每个元素，将返回的迭代器的所有内容构成新的 RDD。通常用来切分单词
    //案例1：将行数据切分为单词
    val lines = sc.parallelize(List("hello world", "hi")) //可以看做将迭代器拍扁
    val words = lines.flatMap(line => line.split(" ")).collect() //words: Array[String] = Array(hello, world, hi)
    //案例2：将多重List切割
    val rdd7 = sc.parallelize(List(List("a b c d ", "s d"), List("s g jh", "d f"), List("s t y", "y d d")))
    rdd7.flatMap(_.flatMap(_.split("\\s+"))).collect()
    // Array[String] = Array(a, b, c, d, s, d, s, g, jh, d, f, s, t, y, y, d, d)

    println("flatMapValues(func)============")
    // 对 pair RDD中的每个值应用一个返回迭代器的函数，然后对返回的每个元素都生成一个对应原键的键值对记录。通常用于符号化
    //案例1：
    sc.parallelize(List((1, 2), (3, 4), (3, 6))).flatMapValues(x => (x to 5))
    //{(1, 2), (1, 3), (1, 4), (1, 5), (3, 4), (3, 5)}
    //案例2：
    sc.parallelize(List(("a", "1 2"), ("b", "3 4"))).flatMapValues(_.split(" ")).collect
    //res29: Array[(String, String)] = Array((a,1), (a,2), (b,3), (b,4))
  }

  val filter_filterByRange = 0
  if (0) {
    println("filter(f: T => Boolean): RDD[T]==============")
    //为RDD的每个元素计算布尔函数，并将函数返回true的项放入生成的RDD中。 注意，每个元素必须都能计算。
    //案例1：
    sc.parallelize(1 to 10, 3).filter(_ % 2 == 0).collect
    //res3: Array[Int] = Array(2, 4, 6, 8, 10)

    println("filterByRange(lower: K, upper: K): RDD[P]===============")
    // 返回仅包含指定键范围中的项的RDD。只有在您的数据是键值对并且已经按键排序时才有效。
    val rdd8 = sc.parallelize(List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"),
      (5, "heater")), 3)
    rdd8.sortByKey().filterByRange(1, 3).collect
    //res66: Array[(Int, String)] = Array((1,screen), (2,cat), (3,book))
  }

  val groupBy_groupByKey = 0
  if (0) {
    println("groupBy=========")
    //  groupBy[K: ClassTag](f: T => K,numPartitions: Int): RDD[(K, Iterable[T])]
    //  注意，分组后，原来的数据全部变为迭代器
    //案例1：
    val rdd9 = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    rdd9.keyBy(_.length).groupBy(_._1).collect
    //Array[(Int, Iterable[(Int, Int)])] = Array((6,CompactBuffer((6,16))),.........

    println("groupByKey=========")
    // groupByKey([numPartitions])
    // 在(K，V)对的数据集上调用时，返回(K，Iterable <V>)对的数据集。 (大量消耗资源)
    //  注意：如果要对每个键执行聚合（例如总和或平均值）进行分组，则使用reduceByKey或aggregateByKey将产生更好的性能。
    //  注意：默认情况下，输出中的并行级别取决于父RDD的分区数。您可以传递可选的numPartitions参数来设置不同数量的任务。
    //  案例1：
    val rdd10 = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    rdd10.keyBy(_.length).groupByKey.collect
    //res11: Array[(Int, Seq[String])] = Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)),
    // (3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle)))
  }

  val reduceByKey = 0
  if (0) {
    /*
    def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]  默认hash分区
    最终调用的都是下面这个方法：
    def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)]  增加分区信息
    当调用(K，V)对的数据集时，返回(K，V)对的数据集，其中使用给定的reduce函数func聚合每个键的值，
    该函数必须是类型(V，V)=> V.与groupByKey类似，reduce任务的数量可通过可选的第二个参数进行配置。
     */
    val rdd11 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    rdd11.map(x => (x.length, x)).reduceByKey(_ + _)
    //res87: Array[(Int, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))
  }

  val combineByKey = 0
  if (0) {
    //—————————————————————combineByKey
    //  combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, partitioner: Partitioner): RDD[(K, C)]
    //  非常高效的实现，通过一个接一个地应用多个聚合器，组合由双组件元组组成的RDD的值。 和reduceByKey是相同的效果

    //案例1：
    val a = sc.parallelize(List("dog", "dog", "gnu", "salmon", "rabbit", "turkey", "bee", "bear", "bee"), 3) //注意dog和bee
    val b = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val c = a.zip(b) //c.getNumPartitions，具有三个分区
    //res3: Array[(String, Int)] = Array((dog,1), (dog,1), (gnu,2), (salmon,2), (rabbi t,2), (turkey,1), (bee,2), (bear,2), (bee,2))

    c.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
    //第一个参数x:原封不动取出来, 第二个参数:是函数, 局部运算, 第三个:是函数, 对局部运算后的结果再做运算
    //x是每个分区中每个key中value中的第一个值, (hello,1)(hello,2)(good,3)-->(hello(1,2),good(3))-->x就是hello的第一个1, good中的3
    //res12: Array[(String, Int)] = Array((rabbit,2), (turkey,1), (bee,4), (gnu,2), (salmon,2), (bear,2), (dog,2))

    c.combineByKey(x => x + 10, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n).collect()
    //第一个参数加10
    //res13: Array[(String, Int)] = Array((rabbit,12), (turkey,11), (bee,14), (gnu,12) , (salmon,12), (bear,12), (dog,12))
    //bee是(2,2)->(12,2),同理dog是(11,1)


    //案例2：
    val a2 = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val b2 = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val c2 = b2.zip(a2) //注意数字在前
    val d2 = c2.combineByKey(List(_), (x: List[String], y: String) => y +: x, (x: List[String], y: List[String]) => x ::: y)
    //流程：
    // 先通过key分组(1,(dog,cat,turkey))，
    // 然后把其中的第一个value：dog变为List(dog)，
    // 然后进行分区运算y::x(y被添加到list的开头)，//注意不能用x::y，y没有这个方法
    // 最终进行跨区x:::y运算。
    d2.collect
    //res16: Array[(Int, List[String])] = Array((1,List(cat, dog, turkey)), (2,List(gnu, rabbit, salmon, bee, bear, wolf)))
    //你知道为什么dog在中间吗？//分区运算后，跨区合并的问题。


    //案例3：在 Scala 中使用combineByKey() 求每个键对应的平均值
    val input = sc.parallelize(Seq(("coffee", 1), ("coffee", 2), ("panda", 3), ("coffee", 9)), 2)
    val result = input.combineByKey(
      (v) => (v, 1), //注意v是value值而不是key值  (1,2,9)->((1,1),2,9)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), //coffee(3,2)，第一个参数是sum，第二个参数是次数
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)) //同上
        .map { case (key, value) => (key, value._1 / value._2.toFloat) } //用到模式匹配
    result.collectAsMap().map(println(_))
    //(coffee,4.0)  (panda,3.0)
  }

  val sortBy_sortByKey = 0
  if (0) {
    println("sortBy===========")
    //————————————————————————sortBy
    //  sortBy[K](f: (T) ⇒ K, ascending: Boolean = true, numPartitions: Int = this.partitions.size)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
    //  此函数对输入RDD的数据进行排序，并将其存储在新的RDD中。
    //  第一个参数要求您指定一个函数，该函数将输入数据映射到您要sortBy的键。
    //  第二个参数（可选）指定是否要按升序(默认)或降序对数据进行排序。
    //案例1：
    val rdd12 = sc.parallelize(Array(5, 7, 1, 3, 2, 1))
    rdd12.sortBy(c => c, true).collect
    //res101: Array[Int] = Array(1, 1, 2, 3, 5, 7)
    rdd12.sortBy(c => c, false).collect //注意这里不能用下划线，不是一个东西。
    //res102: Array[Int] = Array(7, 5, 3, 2, 1, 1)

    val rdd13 = sc.parallelize(Array(("H", 10), ("A", 26), ("Z", 1), ("L", 5)))
    rdd13.sortBy(c => c._1, ascending = true).collect
    //res109: Array[(String, Int)] = Array((A,26), (H,10), (L,5), (Z,1))
    rdd13.sortBy(c => c._2, ascending = true).collect
    //res108: Array[(String, Int)] = Array((Z,1), (L,5), (H,10), (A,26))
    rdd13.sortBy(c => (c._2 + 13) / 2 * 3 % 3) //可以传一个函数

    println("sortByKey=============")
    //————————————————————————sortByKey
    //  sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[P]
    //  此函数对输入RDD的数据进行排序，并将其存储在新的RDD中。输出RDD是一个混洗RDD，因为它存储由已经混洗的reducer输出的数据。
    // 这个功能的实现实际上非常聪明。
    // 首先，它使用范围分区器在混洗RDD中的范围内对数据进行分区。
    // 然后，它使用标准排序机制使用mapPartition单独对这些范围进行排序。

    //案例1：
    val rdd14 = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val rdd15 = sc.parallelize(1 to rdd14.count.toInt, 2)
    val rdd16 = rdd14.zip(rdd15)
    rdd16.sortByKey(true).collect
    //res74: Array[(String, Int)] = Array((ant,5), (cat,2), (dog,1), (gnu,4), (owl,3))
    rdd16.sortByKey(false).collect
    //res75: Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))

    //案例2：
    val rdd17 = sc.parallelize(1 to 100, 5)
    val rdd18 = rdd17.cartesian(rdd17) //笛卡尔积
    val rdd19 = sc.parallelize(rdd18.takeSample(true, 5, 13), 2) //随机返回一些样本
    rdd19.sortByKey(false).collect()
    //res56: Array[(Int, Int)] = Array((96,9), (84,76), (59,59), (53,65), (52,4))

    //在Scala 中以字符串顺序对整数进行自定义排序
    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.toString.compare(b.toString)

      //override def compare(a: Int, b: Int) = a.compare(b)
    }
    rdd19.sortByKey().collect()
    //Array[(Int, Int)] = Array((12,30), (27,42), (6,73), (75,78), (79,99))
  }

  val keys_keyBy_values = 0
  if (0) {
    //————————————————————keys()
    val rdd20 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    //返回一个仅包含键的RDD
    rdd20.keys
    //{1, 3, 3}

    //————————————————————keyBy
    //  keyBy[K](f: T => K): RDD[(K, T)]
    //  通过对每个数据项应用函数来构造双组件元组（键值对）。
    //  函数的结果成为键，原始数据项成为新创建的元组的值。
    val rdd21 = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    rdd21.keyBy(_.length).collect
    //res26: Array[(Int, String)] = Array((3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant))
    rdd21.keyBy(_ (0)).collect()
    //res42: Array[(Char, String)] = Array((d,dog), (s,salmon), (s,salmon), (r,rat), ( e,elephant))


    //————————————————————values()
    //  返回一个仅包含值的RDD
    val rdd22 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    rdd22.values //{2, 4, 6}
  }

  val 针对两个RDD的连接操作 = 0
  if (0) {
    val rdd = sc.parallelize(Seq(1, 2, 3))
    val other = sc.parallelize(Seq(3, 4, 5))

    // 求并集：生成一个包含两个RDD中所有元素的RDD
    rdd.union(other) //{1, 2, 3, 3, 4, 5}

    // 求交集：两个RDD共同的元素的RDD
    rdd.intersection(other) // {3}

    // 求差集（相当于rdd减去两者的交集）：移除一个 RDD 中的内容
    rdd.subtract(other) //{1, 2}

    // 与另一个RDD的笛卡儿积
    rdd.cartesian(other) //{(1, 3), (1, 4), ... (3, 5)}
  }

  val 针对两个RDD的Join操作 = 0
  if (0) {
    //见JoinDemo
  }

  val 针对两个pairRDD的连接操作 = 0
  if (0) {
    val rdd = sc.parallelize(Seq((1, 2), (3, 4), (3, 6)))
    val other = sc.parallelize(Seq((3, 9))) //注意不要写成Seq(3,9)

    // 删掉RDD中键与other RDD 中的键相同的元素
    rdd.subtractByKey(other) //{(1, 2)}

    // join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
    //对两个RDD进行内连接（只有在两个 pair RDD中都存在的键才输出）
    rdd.join(other) //{(3, (4, 9)), (3, (6, 9))}

    // 对两个 RDD 进行连接操作，确保other的键必须存在（右外连接）
    rdd.rightOuterJoin(other) //{(3,(Some(4),9)), (3,(Some(6),9))}

    // 对两个 RDD 进行连接操作，确保rdd的键必须存在（左外连接）
    rdd.leftOuterJoin(other) //{(1,(2,None)), (3, (4,Some(9))), (3, (6,Some(9)))}

    // cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
    // 一组非常强大的功能，允许使用键将最多3个键值RDD组合在一起。
    //案例1：
    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a.map((_, "b"))
    val c = a.map((_, "c"))
    val d = a.map((_, "d"))
    b.cogroup(c, d).collect
    //Array[(Int, (Iterable[String], Iterable[String], Iterable[String]))] = Array((1,(CompactBuffer(b, b),CompactBuffer(c, c),CompactBuffer(d, d))), (3,(CompactBuffer(b),CompactBuffer(c),CompactBuffer(d))), (2,(CompactBuffer(b),CompactBuffer(c),CompactBuffer(d))))
  }

  val distinct_sample = 0
  if (0) {
    val rdd = sc.parallelize(Seq(1, 2, 3, 3))
    println("distinct===================")
    // 去重，开销很大，需要将所有的数据通过网络进行混洗(shuffle)
    rdd.distinct() //{1, 2, 3}

    println("sample===========")
    //  sample(withRe placement, fraction, [seed])
    //  对RDD采样，以及是否放回（一般是不放回）
    rdd.sample(false, 0.5).collect() //非确定的，并不一定是两个数据，也有可能是0-4个数据
    rdd.sample(true, 0.5).collect() //放回去，有可能会获得两个1
  }

  val repartition_coalesce = 0
  if (0) {
    val rdd = sc.parallelize(Seq(1, 2, 3, 3))
    //参考： https://www.cnblogs.com/lillcol/p/9889162.html
    println("repartition=========")
    //  repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
    //  此函数将分区数更改为numPartitions参数指定的数
    val rdd23 = sc.parallelize(List(1, 2, 10, 4, 5, 2, 1, 1, 1), 3)
    rdd23.partitions.length //3
    rdd23.getNumPartitions
    rdd.repartition(5).partitions.length //5

    println("coalesce============")
    // coalesce ( numPartitions : Int , shuffle : Boolean = false ): RDD [T]
    // 将关联数据合并到给定数量的分区中。
    // repartition(numPartitions)只是coalesce的缩写(numPartitions，shuffle = true)。
    sc.parallelize(1 to 10, 10).coalesce(2, false).getNumPartitions //2
  }


  val collect_collectAsMap = 0
  if (0) {
    val a = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    //————————————————————————collect(): Array[T]
    //  将RDD转换为Scala数组并返回它。如果提供标准的映射函数（即f = T - > U），则在将值插入结果数组之前应用它。
    a.collect()

    //————————————————————————collectAsMap()
    //  将结果以映射表的形式返回，以便查询
    val a2 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    a2.collectAsMap() //有重复的key以后面的为准
    //res32: scala.collection.Map[Int,Int] = Map(1 -> 2, 3 -> 6)
  }

  val count_countByKey_countByValue = 0
  if (0) {
    //————————————————————count(): Long
    //  RDD中的元素个数
    sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2).count //res2: Long = 4

    //————————————————————countByKey(): Map[K, Long]
    // 计算相同的key有多少个
    sc.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2).countByKey
    //res3: scala.collection.Map[Int,Long] = Map(3 -> 3, 5 -> 1)

    //————————————————————countByValue(): Map[T, Long]
    // 计算相同的元素有多少个，而不是根据第二个值计算
    sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 2, 4, 2, 1, 1, 1, 1, 1)).countByValue
    //res27: scala.collection.Map[Int,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> 6, 2 -> 3, 4 -> 2, 7 -> 1)
  }

  val take_top_takeOrdered_takeSample = 0
  if (0) {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    //—————————————————take(num: Int): Array[T]
    //  提取RDD的前n项并将它们作为数组返回，并且尝试只访问尽量少的分区
    rdd.take(2) //{1, 2}

    //—————————————————top(num: Int)
    // top(num: Int)(implicit ord: Ordering[T]): Array[T]
    // 从 RDD 中返回最大的2个元素
    rdd.top(2) //{4, 3}

    //—————————————————takeOrdered(num) (ordering)
    //  从 RDD 中返回最小的2个元素
    rdd.takeOrdered(2) //{1, 2}

    //—————————————————takeSample
    // 会把数据拉取到driver， 它返回一个Array而不是RDD。
    //  takeSample(withReplacement: Boolean, num: Int, seed: Int): Array[T]
    val data = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    data.takeSample(false, 4) //随机取四个，不相同，拿出来不再放进去。(常用)
    data.takeSample(true, 4) //随机取四个，有相同，拿出来再放回去。
    //返回RDD，取样10%，并不保证一定是一个。常用于处理数据倾斜，
    data.sample(false, 0.1)
    data.sample(false, 0.5).zipWithIndex().countByKey().foreach(println(_))

  }

  val reduce_fold_foldByKey = 0
  if (0) {
    println("reduce=========")
    // reduce(f: (T, T) => T): T
    //  并行整合 RDD 中所有数据，注意：你提供的任何功能都应该是可交换的，以便生成可重现的结果
    //案例：
    val a = sc.parallelize(1 to 100, 3)
    a.reduce(_ + _)
    //res41: Int = 5050

    //————————————————————————fold(zeroValue: T)(op: (T, T) => T): T
    //  聚合每个分区的值。每个分区中的聚合变量使用zeroValue初始化。//为什么只能是Int类型？
    //  注意：使用你的函数对这个初始值进行多次计算不会改变结果（例如 + 对应的0，* 对应的1，或拼接操作对应的空列表）
    //案例1：
    val a2 = sc.parallelize(List(1, 2, 3, 4), 2)
    a2.fold(0)(_ + _) //10
    a2.fold(2)(_ + _) //16     初始值参与每个分区和总的计算。
    a2.fold(2)(_ * _) //192    2*(2*1*2)*(2*3*4)=2*4*24=192


    //————————————————————————foldByKey[Pair]
    //  foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
    //  非常类似于fold，但是为RDD的每个键单独执行折叠。而且初始值不参加跨区运算。
    val a3 = sc.parallelize(List("dog", "catn", "owl", "gnun", "ant"), 2)
    val b3 = a3.map(x => (x.length, x))
    b3.foldByKey("")(_ + _).collect
    // Array[(Int, String)] = Array((4,catngnun), (3,dogowlant))
    b3.foldByKey("——")(_ + _).collect
    b3.foldByKey("PP")(_ + _).collect
    //Array[(Int, String)] = Array((4,——catn——gnun), (3,——dog——owlant))
    // 注意owl和ant中间没——，表明先通过key分组，然后对每个分区进行处理。最后跨区运行时初始值不参加运算。
  }

  val aggregate_aggregateByKey = 0
  if (0) {
    println("aggregate==========")
    //  aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
    //  在每个分区中应用第一个reduce函数，以将每个分区中的数据减少为单个结果。
    //  第二个reduce函数用于将所有分区的不同减少的结果组合在一起以得到一个最终结果。
    //  用户还指定初始值，应用于两个reduce。  两个reduce函数必须是可交换的和关联的。

    //案例0：（看不懂是不是？，先跳过，接着往下看。。）
    //  aggregate() 来计算 RDD 的平均值，来代替 map() 后面接 fold() 的方式
    //  rdd.aggregate((0, 0)) ((x, y) =>  (x._1 + y, x._2 + 1), (x, y) =>  (x._1 + y._1, x._2 + y._2))
    // (9,4)

    //案例1：
    val z = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
    //先用分区标签打出RDD的内容
    z.printItemLoc() //如果加个7，分在第二个分区上。
    //res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3],
    //                             [partID:1, val: 4], [partID:1, val: 5], [partID:1, val: 6])

    //初始值是5，分区0的reduce是max(5,1,2,3)=5，分区1的reduce是max(5,4,5,6)=6，最终跨分区的reduce是5+5+6=16
    z.aggregate(5)(math.max(_, _), _ + _) //16
    z.aggregate(0)(math.max _, _ + _) //9

    //案例2：
    val z2 = sc.parallelize(List("a", "b", "c", "d", "e", "f"), 2)
    z2.aggregate("")(_ + _, _ + _)
    //res115: String = abcdef
    z2.aggregate("x")(_ + _, _ + _)
    //res116: String = xxdefxabc

    //案例3重点：
    val z3 = sc.parallelize(List("12", "23", "345", "4567"), 2)
    z3.aggregate("")((x, y) => math.max(x.length, y.length).toString, (x, y) => x + y)
    //res48: String = 24 或 42       //并行任务哪个执行的快，哪个先返回
    z3.aggregate("")((x, y) => math.min(x.length, y.length).toString, (x, y) => x + y)
    //res142: String = 11     //原理同下

    val z4 = sc.parallelize(List("12", "23", "345", ""), 2)
    z4.aggregate("")((x, y) => math.min(x.length, y.length).toString, (x, y) => x + y)
    //res143: String = 10 或 01
    //第一个分区先调用math.min("".length,"12".length)，返回“0”，然后再调用math.min("0".length,"23".length)，返回“1”
    //第二个分区同理，返回0

    val z5 = sc.parallelize(List("12", "23", "", "345"), 2)
    z5.aggregate("")((x, y) => math.min(x.length, y.length).toString, (x, y) => x + y)
    //res144: String = 11        //同上


    println("aggregateByKey=================")
    //HighPerformance5.2讲了一堆什么鬼？？？
    // aggregateByKey [Pair]
    // 像聚合函数一样工作，除了聚合应用于具有相同键的值。与聚合函数不同，初始值不应用于第二个reduce。
    val pairRDD = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    //让我们来看看分区中的内容
    pairRDD.printItemLoc()
    //res2: Array[String] = Array([partID:0, val: (cat,2)], [partID:0, val: (cat,5)], [partID:0, val: (mouse,4)],
    //                            [partID:1, val: (cat,12)], [partID:1, val: (dog,12)], [partID:1, val: (mouse,2)])

    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect
    //res3: Array[(String, Int)] = Array((dog,12), (cat,17), (mouse,6))

    //初始值不应用于第二个reduce
    pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect
    //res4: Array[(String, Int)] = Array((dog,100), (cat,200), (mouse,200))
  }

  val foreach_foreachPartition = 0
  if (0) {
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    //————————————————————————foreach(func)
    //  对 RDD 中的每个元素使用给定的函数
    rdd.foreach { x => println(x) } //打印每个数据

    //————————————————————————foreachPartition
    //  foreachPartition(f: Iterator[T] => Unit)
    //  为每个分区执行无参数函数。效率更高。通过iterator参数提供对分区中包含的数据项的访问。
    //案例1：
    rdd.foreachPartition(x => println(x.reduce(_ + _)))
    //6  //第一个分区
    //15 //第二个分区
    //24  //第三个分区
  }

  val lookup = 0
  if (0) {
    //——————————————————————————lookup(key)
    //返回给定键对应的所有值
    val a = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    a.lookup(3)
    //res39: Seq[Int] = WrappedArray(4, 6)
  }

  val 持久化 = 0
  //见笔记spark调优-01代码调优

  val 分区Partition = 0
  if (0) {
    import org.apache.spark.HashPartitioner
    import org.apache.spark.RangePartitioner

    println("partitionBy的使用========================================================================")
    if (0) {
      //参考：https://blog.csdn.net/zhangzeyuan56/article/details/80935034
      println("HashPartitioner================")
      //HashPartitioner确定分区的方式：partition = key.hashCode () % numPartitions
      val counts = sc.parallelize(List((2, 'b'), (3, 'c'), (3, "cc"), (2, "bb"), (1, 'a'), (1, "aa")), 3)
      counts.printItemLoc()
      val partition = counts.partitionBy(new HashPartitioner(3))
      partition.printItemLoc()

      println("RangePartitioner==============")
      //RangePartitioner会对key值进行排序，然后将key值划分成3份key值集合。
      val partition2 = counts.partitionBy(new RangePartitioner(3, counts))
      partition2.printItemLoc()

      println("CustomPartitioner===========")
      //CustomPartitioner可以根据自己具体的应用需求，自定义分区。
      //复杂的案例见sparkdemo.practice.Demo06

      class CustomPartitioner(numParts: Int) extends Partitioner {
        //分多少个区
        override def numPartitions: Int = numParts
        //什么数据放到哪个区
        override def getPartition(key: Any): Int = {
          if (key == 2) {
            1
          } else if (key == 3) {
            1
          } else {
            2
          }
        }
      }
      val partition3 = counts.partitionBy(new CustomPartitioner(3))
      partition3.printItemLoc()

      // Spark从HDFS读入文件的分区数默认等于HDFS文件的块数(blocks)，HDFS中的block是分布式存储的最小单元。
      // 如果我们上传一个30GB的非压缩的文件到HDFS，HDFS默认的块容量大小128MB，因此该文件在HDFS上会被分为235块(30GB/128MB)；
      // Spark读取SparkContext.textFile()读取该文件，默认分区数等于块数即235

      // 一般合理的分区数设置为总核数的2~3倍
    }

    println("事先使用partitionBy对RDD进行分区,可以减少大量的shuffle======================================")
    if (0) {
      //如果一个RDD需要多次在join(特别是迭代)中使用,那么事先使用partitionBy对RDD进行分区,可以减少大量的shuffle
      //参考：https://blog.csdn.net/wy250229163/article/details/52388305
      //https://blog.csdn.net/yhb315279058/article/details/50955282

      val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1), (1, 2), (2, 1), (3, 1))).
          partitionBy(new HashPartitioner(2)).persist()
      val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w'), (2, 'y'), (2, 'z'), (4, 'w')))
      //有优化效果,rdd1不再需要shuffle
      val (res1, time1) = getMethodRunTime(rdd1.join(rdd2))
      //有优化效果,rdd1不再需要shuffle
      val (res2, time2) = getMethodRunTime(rdd1.join(rdd2, new HashPartitioner(2)))
      //无优化效果,rdd1需要再次shuffle
      val (res3, time3) = getMethodRunTime(rdd1.join(rdd2, new HashPartitioner(3)))
    }

    println("计算质数通过分区(Partition)提高Spark的运行性能=============================================")
    //https://blog.csdn.net/u012102306/article/details/53101424
    if (0) {
      val n = 2000000
      //val composite = sc.parallelize(2 to n, 8).map(x => (x, 2 to n / x)).flatMap(kv => kv._2.map(_ * kv._1)) //14s
      val composite = sc.parallelize(2 to n, 8).map(x => (x, (2 to (n / x)))).repartition(8).flatMap(kv => kv._2.map(_ * kv._1)) //7s
      val prime = sc.parallelize(2 to n, 8).subtract(composite)
      prime.collect()
      sleepApp()
    }
  }


}
