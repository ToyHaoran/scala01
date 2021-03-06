package scalademo.b_function

/**
  * 对应快学scala第21章隐式转化
  */
object ImplicitDemo extends App {
  // 参考：
  // https://blog.csdn.net/m0_37138008/article/details/78120210
  // https://my.oschina.net/aiguozhe/blog/35968?catalog=115675

  val 隐式转换函数 = 0
  if (1) {
    println("应用于值==========")

    //定义将Int类型的值转换为Float的函数
    implicit def int2float(x: Int): Float = x.toFloat //int2float: (x: Int)Float
    val x: Float = 2 //x: Float = 2.0
    println(x.getClass.getName)

    println("应用于类==========")
    //使用隐式转换丰富现有类库的功能。
    class Dog {
      def park() = println("wangwangwang")
    }
    class RichDog(val from: Dog) {
      def park2() = println("miomiomio")
    }
    implicit def dog2RichDog(from: Dog): RichDog = new RichDog(from)

    //测试
    val dog = new Dog() //dog: Dog = Dog@120d6cbf
    dog.getClass //res14: Class[_ <: Dog] = class Dog
    dog.park() //wangwangwang
    dog.park2() //miomiomio
  }

  val 隐式类与隐式对象 = 0
  if (0) {
    println("隐式类：通过在类名前使用 implicit 关键字定义=============")
    //例子：string中没有bark方法，通过隐式转换，调用对应的方法转换
    implicit class Dog(val name: String) {
      def bark() = println(s"$name is barking")
    }

    "barkdo".bark() //自动转换为一个类。 barkdo is barking

    //注意事项：
    //1.其所带的构造参数有且只能有一个
    //2.隐式类必须被定义在类，伴生对象和包对象里
    //3.隐式类不能是case class（case class在定义会自动生成伴生对象与2矛盾）
    //4.作用域内不能有与之相同名称的标示符

    println("隐式对象（不常用）==========")
  }

  val 隐式参数与隐式值 = 0
  if (0) {
    println("隐式参数和隐式值===========")
    //implicit val 变量名：类型=值
    //参数值隐式传入场景：当函数中有定义隐式参数时（需单独列出来），会在当前作用域寻找同类型的隐式变量/标量，直接传入，无需在调用时传入：

    //定义一个带隐式参数的函数,只能提供一个隐式字符串
    def sayHi(name: String)(implicit words: String) = print("Hi," + name + "!" + words)
    //sayHi: (name: String)(implicit words: String)Unit

    //定义一个隐式值（注意不能定义两个，否则引起歧义）
    implicit val w = " nice to meet you!"
    //w: String = nice to meet you!

    //调用定义的sayHi函数，它将自行调用定义好的隐式值（从冥界召唤。。。）
    sayHi("zhangsan") //Hi,zhangsan! nice to meet you!
  }

  val 使用隐式参数进行隐式转换 = 0


}
