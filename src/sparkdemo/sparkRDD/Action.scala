package sparkdemo.sparkRDD

import utils.BaseUtil.int2boolen
import utils.ConnectUtil

object Action extends App{
    val sc = ConnectUtil.getLocalSC
    val spark = ConnectUtil.getLocalSpark

    val 行动操作 = 0
    val collect_collectAsMap = 0
    if(0){
        val a = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
        //————————————————————————collect(): Array[T]
        //  将RDD转换为Scala数组并返回它。如果提供标准的映射函数（即f = T - > U），则在将值插入结果数组之前应用它。
        a.collect()

        //————————————————————————collectAsMap()
        //  将结果以映射表的形式返回，以便查询
        val a2 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
        a2.collectAsMap()                  //有重复的key以后面的为准
        //res32: scala.collection.Map[Int,Int] = Map(1 -> 2, 3 -> 6)
    }

    val count_countByKey_countByValue = 0
    if(0){
        //————————————————————count(): Long
        //  RDD中的元素个数
        sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2).count  //res2: Long = 4

        //————————————————————countByKey(): Map[K, Long]
        // 计算相同的key有多少个
        sc.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2).countByKey
        //res3: scala.collection.Map[Int,Long] = Map(3 -> 3, 5 -> 1)

        //————————————————————countByValue(): Map[T, Long]
        // 计算相同的元素有多少个，而不是根据第二个值计算
        sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1)).countByValue
        //res27: scala.collection.Map[Int,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> 6, 2 -> 3, 4 -> 2, 7 -> 1)
    }

    val take_top_takeOrdered_takeSample = 0
    if(0){
        val rdd =sc.parallelize(Seq(1,2,3,4))
        //—————————————————take(num: Int): Array[T]
        //  提取RDD的前n项并将它们作为数组返回，并且尝试只访问尽量少的分区
        rdd.take(2)                   //{1, 2}

        //—————————————————top(num: Int)
        // top(num: Int)(implicit ord: Ordering[T]): Array[T]
        // 从 RDD 中返回最大的2个元素
        rdd.top(2)                    //{4, 3}

        //—————————————————takeOrdered(num) (ordering)
        //  从 RDD 中返回最小的2个元素
        rdd.takeOrdered(2)              //{1, 2}

        //—————————————————takeSample
        // 会把数据拉取到driver， 它返回一个Array而不是RDD。
        //  takeSample(withReplacement: Boolean, num: Int, seed: Int): Array[T]
        val data = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        data.takeSample(false, 4)//随机取四个，不相同，拿出来不再放进去。(常用)
        data.takeSample(true, 4)//随机取四个，有相同，拿出来再放回去。
        //返回RDD，取样10%，并不保证一定是一个。常用于处理数据倾斜，
        data.sample(false, 0.1)
        data.sample(false, 0.5).zipWithIndex().countByKey().foreach(println(_))

    }

    val reduce_fold_foldByKey = 0
    if(0){
        //————————————————————————reduce(f: (T, T) => T): T
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
        a2.fold(0)(_ + _)//10
        a2.fold(2)(_ + _)//16     初始值参与每个分区和总的计算。
        a2.fold(2)(_ * _)//192    2*(2*1*2)*(2*3*4)=2*4*24=192


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
    if(0){
        //——————————————————————————aggregate
        //  aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
        //  在每个分区中应用第一个reduce函数，以将每个分区中的数据减少为单个结果。
        //  第二个reduce函数用于将所有分区的不同减少的结果组合在一起以得到一个最终结果。
        //  用户还指定初始值，应用于两个reduce。  两个reduce函数必须是可交换的和关联的。

        //案例0：（看不懂是不是？，先跳过，接着往下看。。）
        //  aggregate() 来计算 RDD 的平均值，来代替 map() 后面接 fold() 的方式？？？？
        //  rdd.aggregate((0, 0)) ((x, y) =>  (x._1 + y, x._2 + 1), (x, y) =>  (x._1 + y._1, x._2 + y._2))                (9,4)

        //案例1：
        val z = sc.parallelize(List(1,2,3,4,5,6), 2)
        //先用分区标签打出RDD的内容
        def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
            iter.map(x => "[partID:" + index + ", val: " + x + "]")
        }
        z.mapPartitionsWithIndex(myfunc).collect     //如果加个7，分在第二个分区上。
        //res28: Array[String] = Array([partID:0, val: 1], [partID:0, val: 2], [partID:0, val: 3],
        //                             [partID:1, val: 4], [partID:1, val: 5], [partID:1, val: 6])

        //初始值是5，分区0的reduce是max(5,1,2,3)=5，分区1的reduce是max(5,4,5,6)=6，最终跨分区的reduce是5+5+6=16
        z.aggregate(5)(math.max(_, _), _ + _)//16
        z.aggregate(0)(math.max(_, _), _ + _)//9

        //案例2：
        val z2 = sc.parallelize(List("a","b","c","d","e","f"),2)
        z2.aggregate("")(_ + _, _+_)
        //res115: String = abcdef
        z2.aggregate("x")(_ + _, _+_)
        //res116: String = xxdefxabc

        //案例3重点：
        val z3 = sc.parallelize(List("12","23","345","4567"),2)
        z3.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)
        //res48: String = 24 或 42       //并行任务哪个执行的快，哪个先返回
        z3.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
        //res142: String = 11     //原理同下

        val z4 = sc.parallelize(List("12","23","345",""),2)
        z4.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
        //res143: String = 10 或 01
        //第一个分区先调用math.min("".length,"12".length)，返回“0”，然后再调用math.min("0".length,"23".length)，返回“1”
        //第二个分区同理，返回0

        val z5 = sc.parallelize(List("12","23","","345"),2)
        z5.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y)
        //res144: String = 11        //同上



        //——————————————————————————aggregateByKey [Pair]
        //  像聚合函数一样工作，除了聚合应用于具有相同键的值。与聚合函数不同，初始值不应用于第二个reduce。
        val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
        //让我们来看看分区中的内容
        def myfunc2(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
            iter.map(x => "[partID:" + index + ", val: " + x + "]")
        }
        pairRDD.mapPartitionsWithIndex(myfunc2).collect
        //res2: Array[String] = Array([partID:0, val: (cat,2)], [partID:0, val: (cat,5)], [partID:0, val: (mouse,4)],
        //                            [partID:1, val: (cat,12)], [partID:1, val: (dog,12)], [partID:1, val: (mouse,2)])

        pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect
        //res3: Array[(String, Int)] = Array((dog,12), (cat,17), (mouse,6))

        //初始值不应用于第二个reduce
        pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect
        //res4: Array[(String, Int)] = Array((dog,100), (cat,200), (mouse,200))
    }

    val foreach_foreachPartition = 0
    if(0){
        val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
        //————————————————————————foreach(func)
        //  对 RDD 中的每个元素使用给定的函数
        rdd.foreach { x => println(x) }   //打印每个数据

        //————————————————————————foreachPartition
        //  foreachPartition(f: Iterator[T] => Unit)
        //  为每个分区执行无参数函数。效率更高。通过iterator参数提供对分区中包含的数据项的访问。
        //案例1：
        rdd.foreachPartition(x => println(x.reduce(_ + _)))
        //6  //第一个分区
        //15 //第二个分区
        //24  //第三个分区
    }

    val 不常用_lookup = 0
    if(0){
        //——————————————————————————lookup(key)
        //返回给定键对应的所有值
        val a = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
        a.lookup(3)
        //res39: Seq[Int] = WrappedArray(4, 6)
    }

    val 持久化 = 0




}