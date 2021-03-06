package scalademo.b_function

object loopDemo extends App {

  val while循环 = 0
  if (false) {
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
  if (false) {
    print("to包括最后一个====")
    (0 to 20).mkString(sep = " ")
    //每两个取一个
    (0 to(20, 2)).mkString(sep = " ")

    println("until不包括最后一个=====")
    (0 until 20).mkString(sep = " ")
    (0 until(20, 2)).mkString(sep = " ")
  }

  val 高级for循环 = 0
  if (false) {
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
  if (false) {
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

  if (false) {
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

  if (false) {
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
