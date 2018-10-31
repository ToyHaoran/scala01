package scalademo

import utils.BaseUtil.int2boolen

object loopDemo extends App{

    val while循环 = 0
    if(0){
        var flag = true
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

    val do_while循环 = 0

    val for循环 = 0
    if (0) {
        for (i <- 1 until 10; star = 4; j <- star to 10 if i != j) {
            println(i * j)
        }

        println("使用yield将for返回值作为一个变量存储===========")
        val res = for (i <- List(1, 3, 4, 5, 7, 5, 4, 5, 34) if i < 7; if i != 7) yield i + 2
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
    if (0) {
        // while循环
        var flag = true
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

object ControlDemo extends App{

    val if表达式是有值的 = 0
    if(0){
        val ran = scala.util.Random.nextInt(3)
        if(ran == 0){
            println("0")
        }else if(ran == 1){
            println("1")
        }else{
            println("其他")
        }
    }

    val scala没有switch语句 = 0
    //但是他有强大的模式匹配机制。

    val 输入和输出 = 0
    if(0){
        println("输出函数========")
        println("Answer:" + 42)
        val name = "lihaoran"
        val age = 24
        println(s"name:$name age:$age")

        println("输入函数======")
        import scala.io.StdIn._
        val name2 = readLine("输入你的名字：") //Enter键结束
        val age2 = readInt()
        println(s"name:$name2 age:$age2")
    }

}

object FunctionDemo {

    val 简单递归函数 = 0
    if(0){
        println("求n的阶乘，递归函数必须指定返回值类型=======")
        def fac(n: Int): Int = if (n <= 0) 1 else n * fac(n - 1)
    }

    val 默认参数和带名参数 = 0
    if(0){
        println("默认参数========")
        def decorate(str:String, left:String = "[", right:String = "]") = left + str + right
        println(decorate("hello"))

        println("带名参数=======")
        println(decorate("hello", right = " lihaoran"))
    }

    val 变长参数 = 0
    if(0){
        def sum(args: Int*) ={
            var res = 0
            for(arg <- args) res += arg
            res
        }

        val s = sum(1,2,3,4,5)
        val s1 = sum(1 to 5: _*) //将区间当做参数序列处理

        println("递归相加====")
        def recursiveSum(args: Int*): Int = {
            if(args.isEmpty){
                0
            }else {
                args.head + recursiveSum(args.tail:_*)
            }
        }
        recursiveSum(1,2,3,4)
    }

    val 过程 = 0
    if(0){
        //和函数相比就是少了一个 = 号
        //没什么用，有时还会出错，还不如写成函数，返回Unit
        def sayHello(name: String) {
            println(name + " 你好")
        }
        sayHello("lihaoran")
    }

    val 懒值 = 0
    if(0){
        lazy val word = scala.io.Source.fromFile("/ssss/sss").mkString
        //故意拼错文件名，初始化语句时并不会报错，不过一旦访问word就会提示：文件未找到。
        //是开发懒数据结构的基础，对于开销较大的初始化语句十分有用。
        //可以看做是val和def的中间状态
    }

    val 异常及异常处理 = 0
    if(0){
        try {
            val num = 3 / 0
        } catch {
            case ex: ArithmeticException => println("算术异常")
            case _: Throwable => println("其他异常")
        }finally {
            println("清理工作")
        }
    }
}

object Function2Demo extends App{

    val 作为值的函数 = 0
    if(0){

    }













}