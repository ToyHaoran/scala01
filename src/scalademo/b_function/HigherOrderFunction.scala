package scalademo.b_function

object HigherOrderFunction extends App {

  import scala.math._

  val 作为值的函数 = 0
  if (false) {
    val num = 3.14
    val fun = ceil _
    //fun是一个包含函数的变量
    fun(num)
    Array(3.14, 1.42, 2.0).map(fun)
  }

  val 匿名函数 = 0
  if (false) {
    Array(3.14, 1.42, 2.0).map((x: Double) => 3 * x)
  }

  val 带函数参数的函数 = 0
  if (false) {
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
  if (false) {
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
  if (false) {
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
  if (false) {
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
