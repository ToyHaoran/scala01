package scalademo.b_function


object FunctionDemo extends App {
  val 简单递归函数 = 0
  if (false) {
    println("求n的阶乘，递归函数必须指定返回值类型=======")
    def fac(n: Int): Int = if (n <= 0) 1 else n * fac(n - 1)
    println(fac(10))
  }

  val 默认参数和带名参数 = 0
  if (false) {
    println("默认参数========")
    def decorate(str: String, left: String = "[", right: String = "]") = left + str + right
    println(decorate("hello"))

    println("带名参数=======")
    println(decorate("hello", right = " lihaoran"))
  }

  val 变长参数 = 0
  if (false) {
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
  if (false) {
    def sayHello(name: String) {
      println(name + " 你好")
    }
    sayHello("lihaoran")
  }

  val 懒值 = 0
  if (false) {
    //故意拼错文件名，初始化语句时并不会报错，不过一旦访问word就会提示：文件未找到。
    //是开发懒数据结构的基础，对于开销较大的初始化语句十分有用。比如说：Connect.spark，以及BaseUtil.spark
    //可以看做是val和def的中间状态
    lazy val word = scala.io.Source.fromFile("/ssss/sss").mkString
  }
}
