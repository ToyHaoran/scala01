package scalademo

import utils.BaseUtil.int2boolen

import scala.beans.BeanProperty

/**
  * User: lihaoran 
  * Date: 2019/1/22
  * Time: 10:54 
  * Description:
  */
object ExtendsDemo extends App {
  val 继承类演示 = 0
  if (0) {
    class Person {
      @BeanProperty var name: String = _

      //只有子类能用的字段，和java不同，在包中不可见。
      protected var age: Int = _

      def this(name: String) {
        this() //每个辅助构造器必须从其他辅助构造器或主构造器开始，即：最终必须调用主构造器
        this.name = name
      }

      def this(name: String, age: Int) {
        this(name)
        this.age = age
      }

      override def toString = {
        s"name:$name  age:$age "
      }
    }

    class Employee(name: String, /*为什么不行。。。override var */ age: Int, val salary: Double) extends Person(name, age) {
      /*
        感觉上面一大堆还不如用this层次分明呢。。
        override覆写父类的val字段。
        见笔记图片
       */

      /*val salary: Double = 1000.0*/
      //调用超类的方法使用super
      override def toString = super.toString + s"salary:$salary "
    }

    class Manager(name: String, age: Int, salary: Double, val empNum: Int) extends Employee(name, age, salary) {
      /*val empNum = 10*/
      override def toString = super.toString + s"empNum:$empNum"

      //比较两个对象
      final override def equals(other: Any) = {
        val that = other.asInstanceOf[Manager]
        if (null == that) {
          false
        } else {
          (this.name == that.name) && (age == that.age) && (salary == that.salary) && (empNum == that.empNum)
        }
      }
    }

    val p = new Person("lihaoran", 24)
    val e = new Employee("lisi", 30, 2222.9)
    val m = new Manager("zhaoliu", 34, 4444.2, 10)
    val m3 = new Manager("zhaoliu", 33, 3343, 12)

    //判断两个对象是否相等。
    println(m.equals(m3))

    if (m.isInstanceOf[Employee]) {
      //将其转为子类的引用
      val s = m.asInstanceOf[Employee]
      println(s.toString)
    }

    if (e.getClass == classOf[Employee]) {
      //指向一个对象而又不是其子类
      println(e.toString)
    }

    //推荐使用模式匹配
    e match {
      case s: Employee => println("sss")
      case _ => println("ddd")
    }
  }

  val 抽象类和字段 = 0
  if (0) {
    abstract class AbstractPerson(val name: String) {
      //抽象字段
      var age: Int
      val phone: Long

      def getId: Int //没有方法体：这个一个抽象方法
    }

    class Employee2(name: String, val phone: Long) extends AbstractPerson(name) {
      var age = 20

      def getId = name.hashCode //不需要override关键字
    }
  }

  val 构造顺序和提前定义 = 0
  if (0) {
    class Creature {
      val range: Int = 10
      val env: Array[Int] = new Array[Int](range)
    }
    class Ant extends Creature {
      //蚂蚁是近视的。
      override val range = 2
    }

    val ant = new Ant
    //env为0怎么办？
    println(ant.env.length)

    class Ant2 extends {
      override val range = 2
    } with Creature
    //使用提前定义，在构造器初始化之前初始化子类的val字段。
    println((new Ant2).env.length)
  }

}





