package sparkdemo.sparkRDD

import utils.BaseUtil.int2boolen
import org.apache.spark.{SparkConf, SparkContext}

object Opporation extends App {

    val sc = utils.ConnectUtil.getSparkLocal

    // 参考：http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html
    val 创建RDD的两种方式 = 0
    if (0) {
        //Scala中的parallelize() 方法
        val lines = sc.parallelize(List("hello  scala", "hello java", "hello scala"))
            .flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2).collect

        val lines2 = sc.textFile("/path/to/README.md")
    }

    val 转化操作 = 0
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
        def myfunc(index: Int, iter: Iterator[Int]): Iterator[String] = {
            iter.map(x => "（分区" + index + "：" + x + ")")
        }
        rdd6.mapPartitionsWithIndex(myfunc).collect()
        //res57: Array[String] = Array(（分区0：1), （分区0：2), （分区0：3),
        // （分区1：4), （分区1：5), （分区1：6),
        // （分区2：7), （分区2：8), （分区2：9), （分区2：10))
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
        //——————————————————————————reduceByKey
        //  reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
        //  当调用(K，V)对的数据集时，返回(K，V)对的数据集，其中使用给定的reduce函数func聚合每个键的值，
        //  该函数必须是类型(V，V)=> V.与groupByKey类似，reduce任务的数量可通过可选的第二个参数进行配置。
        //案例1：
        val rdd11 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
        rdd11.map(x => (x.length, x)).reduceByKey(_ + _).collect
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
        rdd13.sortBy(c => c._1, true).collect
        //res109: Array[(String, Int)] = Array((A,26), (H,10), (L,5), (Z,1))
        rdd13.sortBy(c => c._2, true).collect
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
    if(0){
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
        rdd21.keyBy(_(0)).collect()
        //res42: Array[(Char, String)] = Array((d,dog), (s,salmon), (s,salmon), (r,rat), ( e,elephant))


        //————————————————————values()
        //  返回一个仅包含值的RDD
        val rdd22 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
        rdd22.values  //{2, 4, 6}
    }

    val 针对两个RDD的连接操作 = 0
    if (0) {
        val rdd = sc.parallelize(Seq(1, 2, 3))
        val other = sc.parallelize(Seq(3, 4, 5))
        //————————————————————union()
        // 求并集：生成一个包含两个RDD中所有元素的RDD
        rdd.union(other) //{1, 2, 3, 3, 4, 5}

        //————————————————————intersection()
        // 求交集：两个RDD共同的元素的RDD
        rdd.intersection(other) // {3}

        //————————————————————subtract()
        // 求差集（相当于rdd减去两者的交集）：移除一个 RDD 中的内容
        rdd.subtract(other) //{1, 2}

        //————————————————————cartesian()
        // 与另一个RDD的笛卡儿积
        rdd.cartesian(other) //{(1, 3), (1, 4), ... (3, 5)}
    }

    val 针对两个pairRDD的连接操作 = 0
    if(0){
        val rdd = sc.parallelize(Seq((1, 2), (3, 4), (3, 6)))
        val other = sc.parallelize(Seq((3,9)))//注意不要写成Seq(3,9)

        //————————————————————————subtractByKey
        // 删掉RDD中键与other RDD 中的键相同的元素
        rdd.subtractByKey(other)                //{(1, 2)}

        //————————————————————————join
        // join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
        //对两个RDD进行内连接（只有在两个 pair RDD中都存在的键才输出）
        rdd.join(other)                         //{(3, (4, 9)), (3, (6, 9))}

        //————————————————————————rightOuterJoin
        // 对两个 RDD 进行连接操作，确保other的键必须存在（右外连接）
        rdd.rightOuterJoin(other)               //{(3,(Some(4),9)), (3,(Some(6),9))}

        //————————————————————————leftOuterJoin
        // 对两个 RDD 进行连接操作，确保rdd的键必须存在（左外连接）
        rdd.leftOuterJoin(other)                 //{(1,(2,None)), (3, (4,Some(9))), (3, (6,Some(9)))}

        //————————————————————————cogroup
        // cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
        // 一组非常强大的功能，允许使用键将最多3个键值RDD组合在一起。
        //案例1：
        val a = sc.parallelize(List(1, 2, 1, 3), 1)
        val b = a.map((_, "b"))
        val c = a.map((_, "c"))
        b.cogroup(c).collect
        //res7: Array[(Int, (Iterable[String], Iterable[String]))] =
        // Array((2,(ArrayBuffer(b),ArrayBuffer(c))), (3,(ArrayBuffer(b),ArrayBuffer(c))), (1,(ArrayBuffer(b, b),ArrayBuffer(c, c))) )
    }

    val distinct_sample_repartition_coalesce = 0
    if (0) {
        val rdd = sc.parallelize(Seq(1, 2, 3, 3))
        println("distinct===================")
        // 去重，开销很大，需要将所有的数据通过网络进行混洗(shuffle)
        rdd.distinct() //{1, 2, 3}

        println("sample===========")
        //  sample(withRe placement, fraction, [seed])
        //  对RDD采样，以及是否替换
        rdd.sample(false, 0.5) //非确定的

        println("repartition=========")
        //  repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
        //  此函数将分区数更改为numPartitions参数指定的数
        val rdd23 = sc.parallelize(List(1, 2, 10, 4, 5, 2, 1, 1, 1), 3)
        rdd23.partitions.length //3
        rdd23.getNumPartitions
        rdd.repartition(5).partitions.length //5

        println("coalesce============")
        //  coalesce ( numPartitions : Int , shuffle : Boolean = false ): RDD [T]
        //  将关联数据合并到给定数量的分区中。 repartition(numPartitions)只是coalesce的缩写(numPartitions，shuffle = true)。
        sc.parallelize(1 to 10, 10).coalesce(2, false).getNumPartitions //2
    }


}