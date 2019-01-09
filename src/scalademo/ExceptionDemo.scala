package scalademo

import java.io.{FileNotFoundException, FileReader, IOException}

import utils.BaseUtil._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/13
  * Time: 15:03 
  * Description:
  */
object ExceptionDemo extends App {

  val 普通的异常处理 = 0
  if (0) {
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException =>
        println("Missing file exception===============")
        println(ex) //就打印第一行信息。  java.io.FileNotFoundException: input.txt (系统找不到指定的文件。)
        println("1 " + ex.getCause) //null
        println("2 " + ex.getMessage) // 2 input.txt (系统找不到指定的文件。)
        println("3 " + ex.getLocalizedMessage) //同上
        println("4 " + ex.getStackTrace.mkString("\n")) //同下
        ex.printStackTrace()
        println("=" * 40)
        //下面的这段代码用来简化异常信息。
        for (x <- ex.getStackTrace) {
          val classname = x.getClassName
          if (classname.startsWith("org.apache") | classname.startsWith("scala.") | classname.startsWith("java.") |
              classname.startsWith("javax.") | classname.startsWith("sun.") | classname.startsWith("oracle.jdbc.")) {
            print("==\t")
          } else {
            print(x.getFileName + ":" + x.getLineNumber)
          }
          println("\t[" + x.getClassName + "]" + x.getLineNumber + " :" + x.getMethodName)
        }
        println()

      case ex: IOException =>
        println("IO Exception")
        ex.printStackTrace()

    } finally {
      println("无论怎么样，最终都要退出=========")
    }
  }

  /*
  用try-catch的模式,异常必须在抛出的时候马上处理.
  然而在分布式计算中,我们很可能希望将异常集中到一起处理,来避免需要到每台机器上单独看错误日志的窘态.
  Try实例可以序列化,并且在机器间传送.
  Try有类似集合的操作 filter, flatMap, flatten, foreach, map, get, getOrElse, orElse方法
  toOption可以转化为Option
  recover，recoverWith，transform可以让你优雅地处理Success和Failure的结果
   */
  val 用Try保存异常 = 0
  if (0) {
    import scala.util.{Try, Success, Failure}

    println("案例1===================")
    val seq = Seq(0, 1, 2, 3, 4)
    val seqTry = seq.map(x => {
      Try(20 / x)
    })
    //seqTry: Seq[scala.util.Try[Int]] = List(Failure(java.lang.ArithmeticException: devide by zero),Success(20), Success(10), Success(6), Success(5))
    val succSeq = seqTry.flatMap(_.toOption)
    //succSeq: Seq[Int] = List(20, 10, 6, 5) Try可以转换成Option
    val succSeq2 = seqTry.collect {
      case Success(x) => x
    }
    //succSeq2: Seq[Int] = List(20, 10, 6, 5) 和上一个是一样的
    val failSeq: Seq[Throwable] = seqTry.collect {
      case Failure(e) => e
    }
    //failSeq: Seq[Throwable] = List(java.lang.ArithmeticException: devide by zero)


    println("案例2====================")
    def divideBy(x: Int, y: Int): Try[Int] = {
      Try(x / y)
    }

    println(divideBy(1, 1).getOrElse(0)) // 1
    println(divideBy(1, 0).getOrElse(0)) //0
    divideBy(1, 1).foreach(println) // 1
    divideBy(1, 0).foreach(println) // no print

    divideBy(1, 0) match {
      case Success(i) => println(s"Success, value is: $i")
      case Failure(s) => println(s"Failed, message is: $s")
    }
    //Failed, message is: java.lang.ArithmeticException: / by zero
  }

  /*
  Option实际上有3个类型：Option、Some和None，Some和None都是Option的子类型。
  Option表示可选的值，它的返回类型是scala.Some或scala.None。Some代表返回有效数据，None代表返回空值。

  应用场景：map.get("xxx")，得到null，
    第一个问题：是说找不到还是对应的值就是null；
    第二个问题：然后我把判断条件写了一百遍。if(xxx != null)
   */
  val 用Option解决null问题 = 0
  if (1) {
    println("案例1======================")
    val a: Option[String] = Some("1024")
    val b: Option[String] = None
    a.map(_.toInt) //res0: Option[Int] = Some(1024)
    b.map(_.toInt) //res1: Option[Int] = None,不会甩exception
    a.filter(_ == "2048") //res2: Option[String] = None
    b.filter(_ == "2048") //res3: Option[String] = None
    a.getOrElse("2048") //res4: String = 1024
    b.getOrElse("2048") //res5: String = 2048
    a.map(_.toInt).map(_ + 1).map(_ / 5).map(_ / 5 == 41) //res6: Option[Boolean] = Some(true)
    b.map(_.toInt).map(_ + 1).map(_ / 5).map(_ / 2 == 0) //res7: Option[Boolean] = None
    //如果是null,恐怕要一连check null四遍了

    println("案例2=====================")
    val a2: Seq[String] = Seq("1", "2", "3", null, "4")
    val b2: Seq[Option[String]] = Seq(Some("1"), Some("2"), Some("3"), None, Some("4"))
    a2.filter(_ != null).map(_.toInt)
    //res0: Seq[Int] = List(1, 2, 3, 4)
    //如果你忘了检查,编译器是看不出来的,只能在跑崩的时候抛异常
    b2.flatMap(_.map(_.toInt))
    //res1: Seq[Int] = List(1, 2, 3, 4)
    //option帮助你把错误扼杀在编译阶段，flatMap则可以在过滤空值的同时将option恢复为原始数据.

    println("原生容器类对option的支持=============")
    Seq(1, 2, 3).headOption //res0: Option[Int] = Some(1)
    Seq(1, 2, 3).find(_ == 5) //res1: Option[Int] = None
    Seq(1, 2, 3).lastOption //res2: Option[Int] = Some(3)
    Vector(1, 2, 3).reduceLeft(_ + _) //res3: Int = 6
    Vector(1, 2, 3).reduceLeftOption(_ + _) //res4: Option[Int] = Some(6)    //在vector为空的时候也能用
    Seq("a", "b", "c", null, "d").map(Option(_)) //res0: Seq[Option[String]] = List(Some(a), Some(b), Some(c), None, Some(d))
    //原始数据转换成option也很方便

    println("案例3============================")
    val myMap: Map[String, String] = Map("key1" -> "value")
    val value1: Option[String] = myMap.get("key1")
    val value2: Option[String] = myMap.get("key2")
    val value3: String = myMap.getOrElse("key2", "没东西给你")

    println(value1) // Some("value")
    println(value1.get)
    println(value2) // None
    println(value2.isEmpty)
    println(value2.getOrElse("没钱"))
    println(value3) // 没东西给你
  }
}