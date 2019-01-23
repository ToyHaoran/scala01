package scalademo

import utils.BaseUtil.int2boolen

object ControlDemo extends App {

  val 注意点 = 0
  if (0) {
    println("if表达式是有值的。代替了java中的三元运算符。")
    val x1 = 0
    val s1: Int = if (x1 > 0) 1 else 0
    val s11: Any = if (x1 > 0) 1 else () //Unit类型，等于java中的void

    println("块的最后一个表达式的值就是块的值。赋值语句的值是Unit类型（和java不同：java中赋值语句的值是被赋的那个值）。")
    var x2 = 0
    val y2 = x2 = x2 + 1

    println("Scala没有switch语句，但是它有强大的模式匹配机制。")
  }
}

object ParenthesesDemo extends App {

  //人们会笼统地说在函数调用时，小括号和花括号是通用的，但实际上，情况会复杂的多

  /*
  总之：
  大括号{}用于代码块，计算结果是代码最后一行；
  大括号{}用于拥有代码块的函数；
  大括号{}在只有一行代码时可以省略，除了case语句（Scala适用）；
  小括号()在函数只有一个参数时可以省略（Scala适用）；
  几乎没有二者都省略的情况。
  参考foreach--case的括号
   */
  val 介绍小括号和大括号的区别 = 0
  if (true) {
    println("如果你要调用的函数有两个或两个以上的参数，那么你只能使用“小括号”====")
    val add = (x: Int, y: Int) => x + y
    add(1, 2)
    //add{1,2} 报错

    println("如果你要调用的函数只有单一参数，那么“通常”情况下小括号和花括号是可以互换的===")
    val increase = (x: Int) => x + 1
    increase(10)
    increase {
      10
    }

    println("在调用单一参数函数时，小括号和花括号虽然等效，但还是有差异的.===")
    // 如果使用小括号，意味着你告诉编译器：它只接受单一的一行，因此，如果你意外地输入2行或更多，编译器就会报错。
    // 但对花括号来说则不然，如果你在花括号里忘记了一个操作符，代码是可以编辑的，
    // 但是会得到出乎意料的结果，进而导致难以追踪的Bug. 看如下的例子：
    def method(x: Int) = {
      x + 1
    }
    //以下代码可不报错，但是一编译就Waring
    /*def method1(x: Int) = {
        1+
        3
        5
    }*/
    /*
    //报错，1+2是一个完整的表达式，表示1行，3表示另一行
    def method2(x:Int) = (
        1+
        2
        3
        )*/

    println("在调用一个单一参数函数时，如果参数本身是一个通过case语句实现的 “偏函数”，你只能使用“花括号”")
    // 究其原因，我觉得scala对小括号和花括号的使用还是有一种“习惯上”的定位的：
    // 通常人们还是会认为小括号是面向单行的，花括号面向多行的。
    // 在使用case实现偏函数时，通常都会是多个case语句，小括号不能满足这种场合，
    // 只是说在只有一个case语句时，会让人产生误解，认为只有一行，为什么不使用case语句。
    val tupleList = List[(String, String)]()
    //val filtered = tupleList.takeWhile( case (s1, s2) => s1 == s2 ) //报错
    val filtered = tupleList.takeWhile { case (s1, s2) => s1 == s2 }

    println("作为表达式（expression）和语句块（code blocks）时====")
    // 在非函数调用时，小括号可以用于界定表达式，花括号可以用于界定代码块。
    // 代码块由多条语句(statement)组成，每一条语句可以是一个”import”语句，一个变量或函数的声明，或者是一个表达式（expression），
    // 而一个表达式必定也是一条语句（statement），所以小括号可能出现在花括号里面，
    // 同时，语句块里可以出现表达式，所以花括号也可能出现在小括号里。看看下面的例子：
    /*
    1       // literal - 字面量
    (1)     // expression - 表达式
    {1}     // block of code - 代码块
    ({1})   // expression with a block of code - 表达式里是一个语句块
    {(1)}   // block of code with an expression - 语句块里是一个表达式
    ({(1)}) // you get the drift... - 你懂的。。。
     */
  }


}

object loopDemo extends App {

  val while循环 = 0
  if (0) {
    var flag = true
    var n, res = 0
    while (flag) {
      res += n
      n += 1
      println("res = " + res)
      println("n = " + n)
      if (n == 10) {
        flag = false
      }
    }
  }

  val do_while循环 = 0

  val to和until示例 = 0
  if (0) {
    print("to包括最后一个====")
    (0 to 20).mkString(sep = " ")
    //每两个取一个
    (0 to(20, 2)).mkString(sep = " ")

    println("until不包括最后一个=====")
    (0 until 20).mkString(sep = " ")
    (0 until(20, 2)).mkString(sep = " ")
  }

  val 高级for循环 = 0
  if (0) {
    for (i <- 1 until 10; star = 4; j <- star to 10 if i != j) {
      print(i * j + " ")
    }
    println()

    println("使用yield将for返回值作为一个变量存储===========")
    //带了两个守卫.
    val res = for (i <- List(1, 3, 4, 5, 7, 5, 4, 5, 34) if i < 7; if i > 3) yield i + 2
    res.foreach(x => {
      print(x + " ")
    })
    println()

    println("打印正三角99乘法表======")
    for (i <- 1 to 9; j <- 1 to 9) {
      if (i >= j) {
        print(String.format("%-10s", s"${i}x${j}=${i * j} "))
      }
      if (i == j) {
        println()
      }
    }

    println("一句话打印1-100内能被3整除的数的两倍组成的集合===========")
    (for (i <- 1 to 100 if i % 3 == 0) yield i * 2).foreach(arg => print(arg + " "))

  }


  val 退出循环 = 0
  //没有提供break和continue
  if (0) {
    println("方法1：flag控制变量")
    /** demo01 见上面[[while循环]] */
    // for循环
    var res = 0
    var flag = true
    for (i <- 0 until 10 if flag) {
      res += i
      println("res = " + res)
      if (i == 5) flag = false
    }
  }
  if (0) {
    println("方法2：使用嵌套函数以及return")
    //1+2+3+4
    def addOuter() = {
      var res = 0
      def addInner() {
        for (i <- 0 until 10) {
          if (i == 5) {
            return
          }
          res += i
          println("res = " + res)
        }
      }
      addInner()
      res
    }
  }

  if (0) {
    println("方法3：使用Breaks类的break方法")
    //控制权的转移是通过抛出和捕获异常完成的，时间较长
    import scala.util.control.Breaks._
    var res = 0
    breakable {
      for (i <- 0 until 10) {
        if (i == 5) {
          break
        }
        res += i
      }
    }
    println("res = " + res)
  }
}


object FunctionDemo {
  val 简单递归函数 = 0
  if (0) {
    println("求n的阶乘，递归函数必须指定返回值类型=======")
    def fac(n: Int): Int = if (n <= 0) 1 else n * fac(n - 1)
    println(fac(10))
  }

  val 默认参数和带名参数 = 0
  if (0) {
    println("默认参数========")
    def decorate(str: String, left: String = "[", right: String = "]") = left + str + right
    println(decorate("hello"))

    println("带名参数=======")
    println(decorate("hello", right = " lihaoran"))
  }

  val 变长参数 = 0
  if (0) {
    def sum(args: Int*) = {
      var res = 0
      for (arg <- args) res += arg
      res
    }

    val s = sum(1, 2, 3, 4, 5)
    //将区间当做参数序列处理 重点。
    val s1 = sum(1 to 5: _*)

    println("递归相加====")
    def recursiveSum(args: Int*): Int = {
      if (args.isEmpty) {
        0
      } else {
        args.head + recursiveSum(args.tail: _*)
      }
    }
    recursiveSum(1, 2, 3, 4)
  }

  val 过程 = 0
  //就是没有返回值的函数，和函数相比就是少了一个 = 号
  //感觉没什么用，有时还会出错，还不如写成函数，返回Unit
  if (0) {
    def sayHello(name: String) {
      println(name + " 你好")
    }
    sayHello("lihaoran")
  }

  val 懒值 = 0
  if (0) {
    //故意拼错文件名，初始化语句时并不会报错，不过一旦访问word就会提示：文件未找到。
    //是开发懒数据结构的基础，对于开销较大的初始化语句十分有用。比如说：Connect.spark，以及BaseUtil.spark
    //可以看做是val和def的中间状态
    lazy val word = scala.io.Source.fromFile("/ssss/sss").mkString
  }
}

object HigherOrderFunction extends App {

  import scala.math._

  val 作为值的函数 = 0
  if (0) {
    val num = 3.14
    val fun = ceil _
    //fun是一个包含函数的变量
    fun(num)
    Array(3.14, 1.42, 2.0).map(fun)
  }

  val 匿名函数 = 0
  if (0) {
    Array(3.14, 1.42, 2.0).map((x: Double) => 3 * x)
  }

  val 带函数参数的函数 = 0
  if (0) {
    def valueCompute(f: (Double) => Double) = f(0.25)
    //传递一个函数
    valueCompute(ceil _)
    valueCompute(sqrt(_))

    def mulBy(factor: Double) = (x: Double) => factor * x
    //返回一个函数
    val temp = mulBy(5)
    temp(20)
  }

  val 参数类型推断 = 0
  if (0) {
    //匿名函数
    val fun01 = (x: Double) => 3 * x
    //传递函数的函数
    def valueCompute(f: (Double) => Double) = f(0.25)
    //调用--逐渐简化
    valueCompute((x: Double) => 3 * x)
    valueCompute((x) => 3 * x) //自动推断
    valueCompute(x => 3 * x)
    valueCompute(3 * _) //如果参数在=>右侧只出现一次，可以用_替换
  }

  val 常用高阶函数 = 0
  if (0) {
    println("map和foreach===========")
    (1 to 9).map("*" * _).foreach(println(_)) //左三角
    (1 to(18, 2)).map(n => " " * ((18 - n) / 2) + ("*" * n)).foreach(println(_)) //正三角

    //求偶数
    (1 to 9).filter(_ % 2 == 0)
    //所有数的乘积
    (1 to 9).reduceLeft(_ * _)
    (1 to 9).product
  }

  val 闭包 = 0

  val SAM转换 = 0

  val 柯里化 = 0
  if (0) {
    //柯里化就是讲原来接收两个参数的函数变成接受一个参数的函数的过程
    def mul(x: Int, y: Int) = x * y
    def mul2(x: Int) = (y: Int) => x * y
    //调用
    mul(6, 7)
    mul2(6)(7)

    //柯里化
    def mul3(x: Int)(y: Int) = x * y
    val tempFun = mul3(6)(_)
    tempFun(7)

    mul3(6)(7)
  }

}
