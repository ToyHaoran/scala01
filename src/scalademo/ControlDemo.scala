package scalademo

import utils.BaseUtil.int2boolen

object loopDemo extends App{

    val while循环 = 0

    val do_while循环 = 0


    val for循环 = 0
    if (1) {
        for (i <- 1 until 10; star = 4; j <- star to 10 if i != j) {
            println(i * j)
        }

        println("使用yield将for返回值作为一个变量存储===========")
        var res = for (i <- List(1, 3, 4, 5, 7, 5, 4, 5, 34) if i < 7; if i != 7) yield i + 2
        res.foreach(println(_))

        println("打印正三角99乘法表======")
        for (i <- 1 to 9; j <- 1 to 9) {
            if (i >= j) {
                print(String.format("%-10s", i + "x" + j + "=" + (i * j) + " "))
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

    //方法1：flag控制变量
    if (1) {
        // while循环
        var flag = true
        var result = 0
        var n = 0
        var res = 0
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
    if (0) {
        // for循环
        var res = 0
        var flag = true
        for (i <- 0 until 10 if flag) {
            res += i
            println("res = " + res)
            if (i == 5) flag = false
        }
    }

    //方法2：使用嵌套函数以及return
    if (0) {
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

    //方法3：使用Breaks类的break方法
    if (0) {
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
