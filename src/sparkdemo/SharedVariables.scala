package sparkdemo

import org.apache.spark.util.AccumulatorV2
import utils.BaseUtil.int2boolen
import utils.ConnectUtil

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/2
  * Time: 9:05 
  * Description: 共享变量例子
  */
object SharedVariables extends App{
    /*
    笔记：http://4d3810a1.wiz03.com/share/s/1de12x0ssAHg2vQr4b1tgYE_2GfMFF2pgAOY2jEnAv171Pwq

    共享变量：
     通常情况下，一个传递给 Spark 操作（例如 map 或 reduce）的函数 func 是在远程的集群节点上执行的。
     该函数 func 在多个节点执行过程中使用的变量，是同一个变量的多个副本。
     这些变量的以副本的方式拷贝到每个机器上，并且各个远程机器上变量的更新并不会传播回 driver program（驱动程序）。
     通用且支持 read-write（读-写） 的共享变量在任务间是不能胜任的。
     所以，Spark 提供了两种特定类型的共享变量 : broadcast variables（广播变量）和 accumulators（累加器）。
      */
    val sc = ConnectUtil.sc

    val 累加器 = 0
    if (0) {
        //可以通过调用SparkContext.longAccumulator()或SparkContext.doubleAccumulator()来创建数字累加器，以分别累积Long或Double类型的值。
        //然后，可以使用add方法将群集上运行的任务添加到群集中。但是，他们无法读懂它的价值。只有驱动程序可以使用其value方法读取累加器的值。

        println("用于添加数组的元素=========")
        val accum1 = sc.doubleAccumulator("doubleAcc")
        val accum2 = sc.longAccumulator("My Accumulator")
        //accum2: org.apache.spark.util.LongAccumulator = LongAccumulator(id: 0, name: Some(My Accumulator), value: 0)
        sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum2.add(x))
        accum2.value //res4: Long = 10

        println("求空白行的操作========")
        val file = sc.textFile("src/sparkdemo/testfile/hello.txt")
        val blankLines = sc.longAccumulator("blankLines")
        val callSigns = file.flatMap(line => {
            if (line == "") {
                blankLines.add(1)
            }
            line.split(" ")
        })
        println("空白行数为：" + blankLines.value)

        println("在过滤掉RDD中奇数的同时进行计数，最后计算剩下整数的和============")
        val accum = sc.longAccumulator("longAccum") //统计奇数的个数
        val sum = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 2).filter(n => {
                if (n % 2 != 0) accum.add(1L)
                n % 2 == 0
            }).reduce(_ + _)
        println("sum: " + sum) //20
        println("accum: " + accum.value) //5

        //少加的情况：累加器不会改变spark的lazy的计算模型，即在打印的时候像map这样的transformation还没有真正的执行，从而累加器的值也就不会更新。
        //注意前面数据是RDD=sc....，你如果改成(0 to 10),他还是会累加的。比如：(0 to 10).map(x => {accum.add(x);x})
        val numberRDD = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 2).map(n => {
                accum.add(1L)
                n + 1
            })
        println("accum: " + accum.value) //0

        //多加的情况：由于count和reduce都是action类型的操作，触发了两次作业的提交，所以map算子实际上被执行了了两次
        //解决方法：numberRDD.cache().count
        numberRDD.count
        println("accum1:" + accum.value) //9
        numberRDD.reduce(_ + _)
        println("accum2: " + accum.value) //18

        println("自定义累加器=========")
        //实现字符串的拼接

        //类继承extends AccumulatorV2[String, String]，第一个为输入类型，第二个为输出类型
        class MyAccumulator extends AccumulatorV2[String, String] {
            private var res = ""
            //isZero: 当累加器中为空时结束程序，res表示空值，对应不同的累加器，比如0，Nil，""
            override def isZero: Boolean = {
                //res == ""
                res.isEmpty
            }
            //merge: 合并数据
            override def merge(other: AccumulatorV2[String, String]): Unit = other match {
                case o: MyAccumulator => res += o.res
                case _ => throw new UnsupportedOperationException(
                    s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
            }
            //copy: 拷贝一个新的AccumulatorV2
            override def copy(): MyAccumulator = {
                val newMyAcc = new MyAccumulator
                newMyAcc.res = this.res
                newMyAcc
            }
            //value: AccumulatorV2对外访问的数据结果
            override def value: String = res
            //add: 操作数据累加方法实现
            override def add(v: String): Unit = res += v + "-"
            //reset: 重置AccumulatorV2中的数据
            override def reset(): Unit = res = ""
        }

        val myAcc = new MyAccumulator
        sc.register(myAcc, "myAcc")
        //val acc = sc.longAccumulator("avg")
        val nums = Array("1", "2", "3", "4", "5", "6", "7", "8")
        val numsRdd = sc.parallelize(nums)
        numsRdd.foreach(num => myAcc.add(num))
        println(myAcc.value) //1-2-3-4-5-6-7-8-
    }

    val 广播变量 = 0
    /*
    注意：
    广播变量是只读的，不能修改它的值
    spark官方建议超过2G的数据，就不适合使用广播变量，应将变量做成RDD。
    不能将RDD广播出去，RDD不存数据，可以将RDD的结果广播出去，rdd.collect()
    广播变量只能在Driver定义，在Executor端不能修改广播变量的值。
    如果executor端用到了Driver的变量，如果不使用广播变量在Executor有多少task就有多少Driver端的变量副本。
    如果Executor端用到了Driver的变量，如果使用广播变量在每个Executor中只有一份Driver端的变量副本。
        更新广播变量（跳过，好像用到流计算）
     */
    if(0){
        //详细见 utils/BroadcastUtil.scala
        val broadcastVar = sc.broadcast(Array(1, 2, 3))
        broadcastVar.value
    }


}