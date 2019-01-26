package scalademo

import utils.BaseUtil.int2boolen

import scala.beans.BeanProperty

/**
  * User: lihaoran 
  * Date: 2019/1/21
  * Time: 17:01 
  * Description: 用来演示集成案例以及特质。
  */
object ClassDemo extends App {
  val 类的demo = 0
  if (0) {
    class Counter {
      /*
        scala会自动生成set和get方法
          如果字段是私有的，set和get方法也是私有的。
          如果字段为val，只有get方法生成。
          如果不需要任何get和set，将字段声明为private[this]
       */
      private var value = 0
      //对象私有字段，注意是对象，不是类。
      private[this] var xxx = 0

      //类似set方法
      def increment(): Unit = {
        value += 1
      }

      //类似get方法
      def current = value

      //比较两个对象
      def isLess(other: Counter) = value < other.value /*+ other.xxx  访问对象的私有字段是不允许的*/

    }

    val myCounter = new Counter
    myCounter.increment() //对改值器用()
    println(myCounter.current) //对取值器去掉()


  }

  val 构造器 = 0
  if (0) {
    /*
      主构造器的参数直接放在类名之后。
      主构造器会执行类定义中的所有语句。
      通常可以在主构造器中使用默认的参数来避免过多的使用辅助参数。
      不带val、var的参数仅仅是可以被主构造器中的代码访问的参数（方法参数），一旦被方法所引用就会破格升为字段。
      前面加private使主构造器私有化。
     */
    /*private 放在外面可以，这里不行*/ class Person /* private(var name: String, age: Int) */ {
      //加上这个字段会自动生成四个方法 name  name_=  getName  setName
      @BeanProperty var name: String = "lihaoran"
      //字段也可以放在构造参数上Person(@xxxx val age:Int)
      var age: Int = 0

      def this(name: String) {
        this() //每个辅助构造器必须从其他辅助构造器或主构造器开始，即：最终必须调用主构造器
        this.name = name
      }

      def this(name: String, age: Int) {
        this(name)
        this.age = age
      }
    }


    val p1 = new Person() //主构造器
    val p2 = new Person("lisi") //辅助构造器
    val p3 = new Person("wangwu", 34)
    p1.setName("lihaoran")
    println(p1.getName)
  }

  val 伴生对象 = 0
  if (0) {
    /*
    单例对象(伴生对象),唯一缺点是不能提供构造器参数。
    一个object可以扩展类以及一个或多个特质，拥有所有特性。
     */
    object Account {
      private var lastNum = 0

      def apply(initBalence: Double): Account = {
        new Account(newUniqueNum(), initBalence)
      }

      def newUniqueNum(): Int = {
        lastNum += 1
        lastNum
      }
    }

    class Account(val id: Int, initBalence: Double) {
      //类和它的伴生对象可以互相访问，必须存在于同一个源文件中。
      /*val id = Account.newUniqueNum()*/
      private var balance = initBalence

      def deposit(amount: Double): Unit = {
        balance += amount
      }

      def getBalance = balance
    }
    //测试一波


    val acc = Account(1000.0)
    println(acc.id)
    println(acc.getBalance)
  }

  val 枚举 = 0
  if (0) {
    object WeekDay extends Enumeration {
      //这行是可选的，类型别名，在使用import语句的时候比较方便，建议加上
      type WeekDay = Value
      //声明枚举对外暴露的变量类型
      val Mon = Value("1")
      val Tue = Value("2")
      val Wed = Value("3")
      val Thu = Value("4")
      val Fri = Value("5")
      val Sat = Value("6")
      val Sun = Value("7")

      def checkExists(day: String) = this.values.exists(_.toString == day)

      //检测是否存在此枚举值
      def isWorkingDay(day: WeekDay) = !(day == Sat || day == Sun)

      //判断是否是工作日
      def showAll() = this.values.foreach(println) // 打印所有的枚举值
    }
    println(WeekDay.checkExists("8")) //检测是否存在
    println(WeekDay.Sun == WeekDay.withName("7")) //正确的使用方法
    println(WeekDay.Sun == "7") //错误的使用方法
    WeekDay.showAll() //打印所有的枚举值
    println(WeekDay.isWorkingDay(WeekDay.Sun)) //是否是工作日

  }
}





