package scalademo

import java.awt.{Point, Rectangle, Shape}
import java.awt.geom.Area
import java.awt.image.BufferedImage
import java.awt.print.Book
import javax.swing.JComponent

import utils.BaseUtil.int2boolen

import scala.annotation.meta.setter
import scala.collection.mutable.ArrayBuffer
import java.io.File
import javax.imageio.ImageIO

import scala.io.Source


/**
  * User: lihaoran 
  * Date: 2019/1/22
  * Time: 21:16 
  * Description: 对应快学scala18章
  */
object DevelopedTypeDemo extends App {

  class Network {

    class Member(val name: String) {
      //val contact = new ArrayBuffer[Member]
      //类型投影，网络之间的成员混用
      val contact = new ArrayBuffer[Network#Member]
    }

    private val member = new ArrayBuffer[Member]

    def join(name: String) = {
      val m = new Member(name)
      member += m
      m
    }
  }

  val 单例类型 = 0
  if (0) {
    object Title

    /*
    给定任何引用v，你可以得到类型v.type，它有两个可能的值： v 和 null 。
    例如我们想让方法返回this，从而实现方法的串接调用：
    */
    class Document {
      var title: String = ""
      var author: String = ""

      def setTitle(title: String): this.type = {
        this.title = title
        this
      }

      def setAuthor(author: String): this.type = {
        this.author = author
        this
      }

      /*
      如果你想定义一个接受object实例作为参数的方法，你也可以使用单例类型。
       */
      private var useNextArgAs: Any = null

      def set(obj: Title.type): this.type = {
        useNextArgAs = obj
        this
      }

      def to(arg: String) = if (useNextArgAs == Title) {
        title = arg
      } else {
        "xxx"
      }
    }

    // 级联使用
    val article = new Document
    article.setTitle("Whatever Floats Your Boat").setAuthor("Cay Horstmann")

    //子类的级联使用，将Document改为this.Type
    class Book extends Document {
      var chapter: String = ""

      def addChapter(chapter: String): this.type = {
        this.chapter = chapter
        this
      }
    }
    val book = new Book
    book.setTitle("Scala for the Impatient").addChapter("chapter1")
    book.set(Title).to("Scala for the Impatient")
  }

  val 类型投影 = 0
  if (0) {
    //两个不同的类
    val chatter = new Network
    val myFace = new Network
    //你不能将其中一个网络的成员添加到另一个网络，就是不能混着用。
    val fred = chatter.join("Fred")
    val brac = myFace.join("Brac")
    fred.contact += brac //在没有类型投影之前报错
  }

  val 类型别名 = 0
  if (0) {
    /*
    对于复杂类型，你可以用type关键字创建一个简单的别名，这和C/C++的typedef相同。
    类型别名必须被嵌套在类或对象中。它不能出现在Scala文件的顶层
    你可以用一个短小的名字代替很长的类型。
    type关键字也用于在子类中被具体化的抽象类型。
     */
    import scala.collection.mutable._
    class Book {
      type Index = HashMap[String, (Int, Int)]
      var name: Index = _
    }
    type Index2 = scala.collection.mutable.HashMap[String, (Int, Int)]
    val map: Index2 = Map(("a", (1, 2))).asInstanceOf[Index2]
  }

  val 结构类型 = 0
  if (0) {
    /*
    所谓的“结构类型”指的是一组关于抽象方法、字段和类型的规格说明，
    这些抽象方法、字段和类型是满足该规格的类型必须具备的。

    你可以对任何具备append方法的类的实例调用appendLines方法 。
    这比定义一个有appendLines方法的特质更为灵活,因为你不能总是能够将该特质添加到使用的类上。
    但是类型结构背后使用的是反射,而反射调用的开销比较大。
    因此,你应该只在需要抓住那些无法共享一个特质的类的共通行为的时候才使用结构类型。
     */
    type Y = {def append(str: String): Any}
    def appendLines(target: Y,
                    lines: Iterable[String]) {
      for (l <- lines) {
        target.append(l)
        target.append("\n")
      }
    }

    //type是把等号后的命名为别名，要想使用open，可以直接使用X,X里有open
    //如果路径长或表达复杂类时可以用type的方式。
    //C语言的结构体也可以将结构体命名成别名。
    type X = {def open(): Unit}
    //init方法要求传进的对象中有一个open方法,只要传进的对象中有open方法，就可以传进来。而不关心其类型。
    //这个特性类似动态语言的特性
    def init(res: X) = res.open //这里的init是局部的本地方法

    //匿名对象中有个open方法。
    init(new {
      def open() = println("Opened")
    })
    init(new {
      def open() = println("Opened again")
    })
    object A {
      def open() {
        println("A single object Opened")
      }
    }
    init(A)
    class Structural {
      def open() = print("A class instance Opened")
    }
    val structural = new Structural
    init(structural)
  }

  val 复合类型 = 0
  if (0) {
    /*
    复合类型的定义形式如下：
    T1 with T2 with T3 ...
    在这里，要想成为该复合类型的实例，某个值必须满足每一个类型的要求才行。例如：
     */
    val image = new ArrayBuffer[java.awt.Shape with java.io.Serializable]
    val rect = new Rectangle(5, 10, 20, 30)
    image += rect // OK , Rectangle是Serializable的
    //image += new Area(rect)  // error, Area是Shape但不是Serializable的

    //你可以把结构类型的声明添加到简单类型或复合类型。例如：
    //该类型必须既是一个Shape的子类型也是Serializable的子类型，并且还必须有一个带Point参数的contains方法。
    type st1 = Shape with Serializable {def contains(p: Point): Boolean}

    // 从技术上讲， 结构类型
    type st2 = {def append(str: String): Any}
    // 是如下代码的简写
    type st3 = AnyRef {def append(str: String): Any}

    // 复合类型
    type st4 = Shape with Serializable
    // 是如下代码的简写
    type st5 = Shape with Serializable {}

  }

  val 中置类型 = 0
  if (0) {
    /*
    中置类型是一个带有两个类型参数的类型，以“中置”语法表示，类型名称写在两个类型参数之间。例如：
      String Map Int
      // 而不是
      Map[String, Int]
     */
    type x[A, B] = (A, B)
    //之后你就可以写String x Int ，而不是(String,Int)了
    type xx = String x Int x Int
    val test: xx = (("name", 3), 4)
  }

  val 存在类型 = 0
  if (0) {
    /*
    存在类型被加入Scala是为了与Java的类型通配符兼容。存在类型的定义是在类型表达式之后跟上forSome{...}，
    花括号中包含了type和val声明。例如：*/
    type ty1 = Array[T] forSome {type T <: JComponent}
    // 这与类型通配符效果相同
    type ty2 = Array[_ <: JComponent]

    //Scala的类型通配符只不过是存在类型的语法糖。例如：
    type ty3 = Array[_] // 等同于
    type ty4 = Array[T] forSome {type T}

    type ty5 = Map[_, _] // 等同于
    type ty6 = Map[T, U] forSome {type T; type U}

    type ty8 = Map[T, U] forSome {type T; type U <: T}

    // 你也可以在forSome代码块中使用val声明，因为val可以有自己的嵌套类型。例如：
    type ty7 = n.Member forSome {val n: Network}

  }

  val 依赖注入 = 0
  if (0) {
    /*在Scala中，你可以通过特质和自身类型达到一个简单的依赖注入的效果。
    trait Logger {
      def log(msg: String)
    }

    class ConsoleLogger(str: String) extends Logger {
      def log(msg: String) = {
        println("Console: " + msg)
      }
    }

    class FileLogger(str: String) extends Logger {
      def log(msg: String) = {
        println("File: " + msg)
      }
    }

    // 用户认证特质有一个对日志功能的依赖
    trait Auth {
      this: Logger =>
        def login(id: String, password: String): Boolean
    }

    // 应用逻辑有赖于上面两个特质
    trait App {
      this: Logger with Auth => println("hahaha")
    }

    // 组装应用
    object MyApp extends App with FileLogger("test.log") with Auth("users.txt")*/

    /*
    像这样使用特质的组合有些别扭。毕竟，一个应用程序并非是认证器和文件日志器的合体。更自然的表示方式可能是通过实例变量来表示组件。
    蛋糕模式给出了更好的设计。在这个模式当中，你对每个服务都提供一个组件特质，该特质包含：

    任何所依赖的组件，以自身类型表述。
    描述服务接口的特质。
    一个抽象的val，该val将被初始化成服务的一个实例。
    可以有选择性的包含服务接口的实现。
    */

    trait LoggerComponent {

      trait Logger {
        def log(msg: String)
      }

      val logger: Logger

      class FileLogger(file: String) extends Logger {
        def log(msg: String) = {
          println("File: " + msg)
        }
      }

    }

    trait AuthComponent {
      // 让我们可以访问日志器
      this: LoggerComponent =>{
        println("hahah")
      }

      trait Auth {
        def login(id: String, password: String): Boolean
      }

      val auth: Auth

      class MockAuth(file: String) extends Auth {
        override def login(id: String, password: String): Boolean = {
          if(id == password) true else false
        }
      }

    }

    // 使用组件
    object AppComponents extends LoggerComponent with AuthComponent {
      val logger = new FileLogger("test.log")
      val auth = new MockAuth("users.txt")
    }
  }

  val 抽象类型 = 0
  if(0) {
    //类或特质可以定义一个在子类中被具体化的抽象类型。例如：
    trait Reader {
      type Contents

      def read(fileName: String): Contents
    }

    class StringReader extends Reader {
      type Contents = String

      def read(fileName: String) = Source.fromFile(fileName, "UTF-8").mkString
    }

    class ImageReader extends Reader {
      type Contents = BufferedImage
      def read(fileName: String) = ImageIO.read(new File(fileName))
    }
  }
  if(0){
    // 同样的效果可以通过类型参数来实现：
    trait Reader[C] {
      def read(fileName: String): C
    }

    class StringReader extends Reader[String] {
      def read(fileName: String) = Source.fromFile(fileName, "UTF-8").mkString
    }

    class ImageReader extends Reader[BufferedImage] {
      def read(fileName: String) = ImageIO.read(new File(fileName))
    }

    /*
    哪种方式更好呢？Scala经验法则：
    如果类型是在类被实例化时给出，则使用类型参数。
    如果类型是在子类中给出，则使用抽象类型。
    抽象类型可以有类型界定，就和参数类型一样。例如：
     */
    trait Listener {
      type Event <: java.util.EventObject
    }

    // 子类必须提供一个兼容的类型
    trait ActionListener extends Listener {
      type Event = java.awt.event.ActionEvent
    }
  }




}