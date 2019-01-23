package scalademo

import utils.BaseUtil.int2boolen

object MatchDemo {

  val match语句 = 0
  if (0) {
    val ran = scala.util.Random.nextInt(3)
    val res = ran match {
      case 0 => "随机数是0"
      case 1 => "随机数是1"
      case _ => "随机数是2" //使用case _ 等价于default，与switch不同，模式匹配不会意外掉到下一个分支。
      // 如果没有模式能匹配，会抛出MatchError
    }
    println(res)
  }

  val case范围 = 0
  if (0) {
    val ran = scala.util.Random.nextInt(7)
    val res = ran match {
      case 0 => "随机数是0"
      // 守卫可以是任何Boolean条件
      case _ if ran >= 2 && ran <= 5 => "随机数是2-5"
      case _ => "其他"
    }
    println(res)
  }

  val 匹配变量_常量 = 0
  if (0) {
    val str = "hello"
    val E = 'e'
    val o = 'o'
    str.foreach(char01 => {
      //这里必须重新定义一下，foreach在小括号内是识别不了类型的，或者用下面的方式
      val char: Char = char01
      //match是有返回值的
      val res = char match {
        case 'h' => "匹配到h"
        case E => "匹配到e" // 常量是大写的
        case 'o' => "匹配到o" //加单引号是小写常量
        case ch => "匹配到其他字符" + ch //等价于case _
      }
      println(res)
    })
    //或者这样写，用中括号,返回值为unit
    val res = str.foreach {
      case 'h' => "匹配到h"
      case E => "匹配到e" // 常量是大写的
      case 'o' => "匹配到o" //加单引号是小写常量
      case ch => "匹配到其他字符" + ch //等价于case _
    }
  }

  val 匹配类型 = 0
  if (1) {
    val ran = scala.util.Random.nextInt(4)
    val obj = List(1, "hello", BigDecimal(10), 3.4)
    obj(ran) match {
      case x: Int => println("Int类型")
      case y: String => println("String类型")
      case _: BigDecimal => println("BigDecimal类型")
      case _ => println("其他类型")
    }
  }

  val 匹配数组_列表_元组 = 0
  if (1) {
    println("匹配数组======")
    val arr = Array(0, 1, 2, 3)
    arr match {
      case Array(0) => println("1个0元素")
      case Array(0, y) => println(s"两个元素0 和 $y")
      case Array(x, y) => println(s"两个元素$x 和 $y")
      case Array(0, _*) => println("第一个元素是0")
      case _ => "something else"
    }

    println("匹配列表=====")
    val lst = List(1, 2, 3, 4)
    lst match {
      case 0 :: Nil => println("1个0元素")
      case List(x, y) => println(s"两个元素$x 和 $y")
      case 0 :: tail => println("第一个元素是0")
      case _ => "something else"
    }

    println("匹配元组=======")
    val pair = (0, 1)
    pair match {
      case (0, _) => println("0 ...")
      case (y, 0) => println(y + " 0")
      case _ => println("something else")
    }
  }

  val 提取器 = 0
  if (1) {
    println("使用正则表达式提取组=======")
    val pattern = "([0-9]+) ([a-z]+)".r
    "99 bottles" match {
      case pattern(num, item) => println(num + "  " + item)
    }

    println("定义变量时的模式匹配======")
    val (q, r) = BigInt(10) /% 3
    println(q + " " + r)

    println("获取key和value值======")
    val map = Map("i" -> 1, "j" -> 2)
    for ((k, v) <- map) {
      println(k + ":" + v)
    }
  }

  val 样例类 = 0
  if (1) {
    abstract class Amount
    case class Dollar(value: Double) extends Amount
    case class Currency(value: Double, unit: String) extends Amount
    case object Nothting extends Amount
    val amt = new Amount() {}
    amt match {
      case Dollar(v) => "$" + v
      case Currency(_, u) => "Oh nose, I got " + u
      case Nothting => "----"
      case _ => "其他"
    }
  }

}