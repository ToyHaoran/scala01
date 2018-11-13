package scalademo

import java.util.Random

object NoDecompile{
    def main(args: Array[String]) {
        println(version02())
    }

    def version02(): String = {
        var testStr = ""
        try {
            //————————————————————————————————————————————
            testStr = "你好，这个是不能反编译的"
            //这种防止反编译的就是只能防止JDCore，而不能防止CFR，不可能完全防止反编译
            //但是从java到scala是不能自动转换的，网上也没有相应的工具，可以说是反编译的一座大山。
            val lst = List(1,2,3,4)
            print(lst(5))
            //————————————————————————————————————————————
        } catch {
            //case _ => println("异常") //能被编译
            //case e:Exception => e.printStackTrace() //能被反编译
            //case e:Throwable => println(e.getMessage) //能被反编译
            case e:Throwable => e.printStackTrace() //不可以被反编译
        }
        testStr
    }


    def version01() : String={
        var testStr = ""
        try {
            //前面的数字也没关系。后面的new Random也可以为任意的，关键是要有这个语句
            if(654789 == new Random().nextInt()){
                throw new Exception("Exception happened")
            }

            //————————————————————————————————————————————
            //这里写代码，必须放在上段代码之后
            testStr = "你好，这个是不能反编译的"
            //但是这样会覆盖原来的错误信息，比如下面这个IndexOutOfBoundsException，结果只输出Exception。
            //所以在完全测试完成的情况下才能加try catch，否则报错都不知道哪里报的。
            val lst = List(1,2,3,4)
            print(lst(5))
            //————————————————————————————————————————————

        } catch {
            case _ => println("异常") //不可以被反编译
        } finally {
            try {
                if (654789 == new Random().nextInt()) {
                    throw new Exception("Exception happened")
                }
            } catch {
                case _ => println("Exception")
            }
        }
        testStr
    }

}
