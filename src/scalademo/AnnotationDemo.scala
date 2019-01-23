package scalademo

import java.io._

import utils.BaseUtil.int2boolen

import scala.annotation.meta.{beanGetter, beanSetter, getter, setter}
import scala.beans.BeanProperty

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2019/1/21
  * Time: 11:38 
  * Description: 有关注解的Demo类
  */
object AnnotationDemo extends App {
  //什么是注解：可以理解为你插入代码中以便有工具可以对他们进行处理的标签。

  val 生成Scala文档 = 0
  if (0) {
    /** 看[[scala.collection.immutable.HashMap]]类，上面一堆注解 */
  }

  val 检查程序中可能出现的语法问题 = 0
  if (0) {
    //当使用此方法时，给出对应的提示，属于语法检查的范畴
    @deprecated("此方法已过期", "1.0")
    def method(x: Int): Unit = {
      print(x)
    }
    method(10)
  }

  val 类型javabean的注解 = 0
  //todo 扩展一下java注解
  if (0) {
    class Student {
      @BeanProperty val name: String = "nali"
      val age: Int = 9
    }

    val stu = new Student
    stu
  }

  val uncheck的使用 = 0
  if (0) {
    //用于匹配不完整时取消警告信息  没看懂。。。
    def f(x: Option[String]) = (x: @unchecked) match {
      case Some(y) => y
    }
    def g(xs: Any) = xs match {
      case x: List[String@unchecked] => x.head
    }
  }

  val 序列化与反序列化demo = 0
  if (0) {
    //下面的代码没有声明序列化前，编译时不会出问题，但执行时会抛出异常 NotSerializableException 。 声明序列化即可
    class Person extends Serializable {
      //这个拒绝序列化后，反序列化后为null
      var name: String = "zzh"
      //transient注解用于标记变量age不被序列化，反序列化后为0，去掉后为9
      @transient var age: Int = 9
      //volatile注解标记变量name非线程安全
      @volatile var money: String = "10000"

      override def toString = "name=" + name + " age=" + age
    }


    val file = new File("person.out")

    val oout = new ObjectOutputStream(new FileOutputStream(file))
    val person = new Person()
    oout.writeObject(person)
    oout.close()

    val oin = new ObjectInputStream(new FileInputStream(file))
    val newPerson = oin.readObject()
    oin.close()
    println(newPerson)
  }
}




