package scalademo.c_collection

import utils.BaseUtil.getMethodRunTime

import java.util.function.BiFunction

/**
  *  HashMap，最大的问题是线程不安全，但不涉及到同步，效率非常高，在不涉及线程安全的程序中广泛被应用。然而当涉及到多线程作业时，就会出现一些问题。
  *  为了解决这些问题JAVA提供了Hashtable，这是一种整体加锁的数据结构，然而效率不敢恭维。这时就有了ConcurrentHashMap。
  *  一个例子说明三者关系：前提：某个卫生间共有16个隔间。
  *
  *  HashMap：每个隔间都没锁门，有人想上厕所，管理员指给他一个隔间，里面没人的话正常用，里面有人的话把这个人赶出来然后用。
  *  优点，每个人进来不耽误都能用；缺点，每一个上厕所的人都有被中途赶出来的危险。
  *
  *  Hashtable：在卫生间外面安装一个大门，有人想上厕所，问管理员要一个钥匙进门，把门反锁用，用完后出来，把钥匙交换给管理员。
  *  在这个人上厕所期间，其他所有人都必须在外面排号。
  *  优点，每个人都能安心上完厕所；缺点，卫生间外面可能已经出了人命。 =_=
  *
  *  ConcurrentHashMap：在卫生间每个隔间安装门锁，有人想上厕所，管理员指给他一个隔间，进来后这个隔间如果没人在用则直接用，
  *  如果有人正在用，则排号。在这期间其他人会按规则分到不同的隔间，重复上述行为。
  *  优点：每个人都能安心上厕所，外面排队的也被均匀分摊。
  */
object ConcurrentHashMapDemo extends App {
  val conMap = new java.util.concurrent.ConcurrentHashMap[String, String]()
  val conMap2 = new java.util.concurrent.ConcurrentHashMap[String, String]()

  //隐式转换，按条件用不同的方式处理value
  import scala.language.implicitConversions
  implicit def func2java(func: (String, String) => String): BiFunction[String, String, String] =  new BiFunction[String, String, String] {
    override def apply(key: String, value: String) = {
      func(key, value)
    }
  }
  import utils.DataCreateUtil._
  //990万数据
  val data = textCreate(9900000, " ").split("\\s+").filter(!_.trim.isEmpty)

  // 并行测试：常用于读取或者写入HDFS文件的时候。
  // 对同一个数据进行处理,在本机并不会加快(反而会变慢)，但是在集群上明显快上好几倍。可能与电脑CPU核数有关。
  var (res, time) = getMethodRunTime(data.par.foreach {
    /*case (name, age) =>
        println("将每个元素进行解析")*/
    case name =>
      // 存在key就累加计数,不存在就设置value为1
      conMap.compute(name, (key: String, value: String) => if (value == null) "1" else (value.toInt + 1).toString)
  })

  //没有并行
  var (res2, time2) = getMethodRunTime(data.foreach(name => {
    conMap2.compute(name, (key: String, value: String) => if (value == null) "1" else (value.toInt + 1).toString)
  }))


  println("并行时间：" + time) // 并行时间：6644.579184  估计是cup核数的问题
  println("不并行时间：" + time2) // 不并行时间：2327.626717

  //需要将java的集合类转化为scala的集合类
  import scala.collection.JavaConversions._
  for ((key, value) <- conMap.toMap) {
    println(key + " " + value)
  }

  /* 费控代码测试，com/dingxin/entrance/Dlrk.scala:206
  带有par的运行时间========555.031426
  不带par的运行时间========2497.330679
  val (res33, time33) = BaseUtils.getMethodRunTime(temp123.par.foreach {
    case (yg, jldxxKey, res1, res2, res3) =>
      if (yg == 1){ //有功
       // 表示如果key为null，value值设置为xxx,否则设置为yyy，用到了上面的隐式转换
        ygpowerMap.compute(jldxxKey, (key: String, value: String) => if (value == null) res1 + res2 else value + "," + res2)
        ygcjpowerMap.compute(jldxxKey, (key: String, value: String) => if (value == null) res3 else value + "," + res3)
      } else {
        wgpowerMap.compute(jldxxKey, (key: String, value: String) => if (value == null) res1 + res2 else value + "," + res2)
      }
  })

  val (res44, time44) = BaseUtils.getMethodRunTime(temp123.foreach(name => {
    val (yg, jldxxKey, res1, res2, res3) = name  //解析
    if (yg == 1){ //有功
      ygpowerMap33.compute(jldxxKey, (key: String, value: String) => if (value == null) res1 + res2 else value + "," + res2)
      ygcjpowerMap33.compute(jldxxKey, (key: String, value: String) => if (value == null) res3 else value + "," + res3)
    } else {
      wgpowerMap33.compute(jldxxKey, (key: String, value: String) => if (value == null) res1 + res2 else value + "," + res2)
    }
  }))
   */
}