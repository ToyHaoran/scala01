package scalademo

import java.io.PrintWriter

import utils.BaseUtil.int2boolen

/**
  * User: lihaoran 
  * Date: 2019/1/22
  * Time: 14:09 
  * Description: 介绍特质的一些性质，对设计模式有很大的作用。
  */
object TraitDemo extends App {

  class Account {
    var balance: Double = 1000

    def deposit(amount: Double): Unit = {
      balance += amount
    }

    def getBalance = balance
  }

  trait Logged {
    def log(msg: String) {}
  }

  trait Logger {
    //特质中未被实现的方法默认是抽象的
    def log(msg: String)

    //当做富接口使用的特质
    def info(msg: String) {
      log("INFO:" + msg)
    }

    def warn(msg: String) {
      log("WARN:" + msg)
    }

    def severe(msg: String) {
      log("SEVERE:" + msg)
    }
  }

  //添加时间戳的特质
  trait TimstampLogger extends Logged {
    override def log(msg: String): Unit = {
      //实际上Logged的log方法什么也不做，因此super.log实际上是调用的是特质层级中的下一个特质。
      super.log(new java.util.Date() + " " + msg)
    }
  }

  //将长的特质变为短的特质
  trait ShortLogger extends Logged {
    //特质中未被初始化的字段必须在子类中被重写。
    val maxLength: Int = 15

    override def log(msg: String): Unit = {
      super.log(
        if (msg.length <= maxLength) msg else msg.substring(0, maxLength - 3) + "..."
      )
    }
  }

  val 特质介绍 = 0
  if (0) {
    //特质可以同时拥有抽象方法和具体方法（java中没有具体方法）

    //使用with添加多个特质，Logger with Cloneable with Serializable 是一个整体。
    trait ConsoleLogger extends Logger with Cloneable with Serializable {
      def log(msg: String): Unit = println(msg)
    }

    trait TimstampLogger extends Logger {
      //在特质中重写抽象方法
      abstract override def log(msg: String): Unit = {
        super.log(new java.util.Date() + " " + msg)
      }
    }


    class SaveAccount extends Account with ConsoleLogger {
      //取款，如果剩余的钱不足，打印信息到控制台
      def withdraw(amount: Double): Unit = {
        if (amount > balance) {
          //从ConsoleLogger中得到了一个具体的log实现方法，java接口是不可能实现的。
          log(s"余额不足，要取$amount，剩余$balance")
        } else {
          balance -= amount
          log(s"取款成功，取出$amount，剩余$balance")
        }
      }
    }

    val account = new SaveAccount
    account.withdraw(500.0)
    account.withdraw(700.0)
  }

  val 特质调用顺序 = 0
  if (1) {

    trait ConsoleLogger extends Logged {
      override def log(msg: String): Unit = println("最终打印：" + msg)
    }
    class SaveAccount extends Account with Logged {
      //取款，如果剩余的钱不足，打印信息到控制台
      def withdraw(amount: Double): Unit = {
        if (amount > balance) {
          //从ConsoleLogger中得到了一个具体的log实现方法，java接口是不可能实现的。
          log(s"余额不足，要取$amount，剩余$balance")
        } else {
          balance -= amount
          log(s"取款成功，取出$amount，剩余$balance")
        }
      }
    }

    println("带有特质的对象============")
    val acct = new SaveAccount with ConsoleLogger
    acct.withdraw(800)



    println("特质调用的顺序===========")
    val acct1 = new SaveAccount with ConsoleLogger with TimstampLogger with ShortLogger
    val acct2 = new SaveAccount with ConsoleLogger with ShortLogger with TimstampLogger

    /*
     一般来说，特质从最后一个开始被处理。先调用ShortLogger的log方法，然后它的super.log调用的是TimstampLogger的log，
     而TimstampLogger的supper.log调用的是ConsoleLogger的log方法，最终打印。
     */
    acct1.withdraw(500) //Tue Jan 22 15:10:50 CST 2019 取款成功，取出500.0...
    //先调用TimstampLogger的log方法，然后它的super.log调用的是ShortLogger。。。。。
    acct2.withdraw(500) //Tue Jan 22 1...

    println("特质中的抽象字段===========")
    trait ShortLogger2 extends Logged {
      //特质中未被初始化的字段必须在子类中被重写。
      val maxLength: Int

      override def log(msg: String): Unit = {
        super.log(
          if (msg.length <= maxLength) msg else msg.substring(0, maxLength - 3) + "..."
        )
      }
    }

    //那么这个实例就可以截断日志消息了。
    val acct3 = new SaveAccount with ConsoleLogger with ShortLogger2 {
      //也可以在SaveAccount中覆写此字段
      val maxLength = 10
    }
    acct3.withdraw(777)
  }

  val 特质构造顺序 = 0
  if (0) {
    trait FileLogger extends Logger {
      val out = new PrintWriter("app.log") // 构造器的一部分
      out.println("# " + new java.util.Date().toString)

      // 也是构造器的一部分
      def log(msg: String) {
        out.println(msg)
        out.flush()
      }
    }

    /*
    首先调用超类的构造器：Account。
    特质构造器在超类构造器之后、类构造器之前执行，特质由左到右被构造，每个特质中，父特质先被构造
    如果多个特质共有一个父特质，那么那个父特质已经被构造，则不会被再次构造。
    所有特质构造完毕后，子类被构造。

    下面的顺序：Account--Logger(父特质)--FileLogger--ShortLogger--SavingsAccount
      class SavingsAccount extends Account with FileLogger with ShortLogger
     */

  }


}