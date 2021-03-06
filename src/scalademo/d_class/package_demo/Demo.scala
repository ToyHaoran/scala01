//重命名包
import utils.{BaseUtil => hahaha}

//隐藏包中的printMap
//另外utils包中的其他类不可见，比下面那种方式好。
import utils.BaseUtil.{printMap => _, _}

/**
  * Description: 源文件的目录和包之间并没有强制的关联关系。但是会警告。
  */
package scalademo.d_class {
  package scalademo.d_class.package_demo {

    //包对象，所有子包可以用的函数，变量等。
    package object People02 {
      val defalutName = "lihaoran"

      def getName = defalutName
    }

    object Demo02 extends App {
      //println(package02.Demo03.name) //会报错
      println(package02.Demo04.age) //正常访问
      println(People02.getName)

      //引用重命名的包
      hahaha.sleepApp(1000)
      //隐藏printMap，其他的都可以用
      if (1) {
        println("hahaha")
      }
    }

    package package01 {

      //在out.production下面就可以看到实际上将文件打到了package01下面
      object Demo extends App {
        //获取上层包对象中的属性
        val name = People02.defalutName
        println("hello world")
        println(name)
      }

    }

    package package02 {

      class Demo03 {
        //只能在package02下面用，否则报错
        private[package02] val name = "lihaoran"
      }

      object Demo04 {
        //将其可见度延展到上层包
        private[package_demo] val age = 15
      }

    }

  }

}

