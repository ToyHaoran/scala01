package scalademo

import java.util.Random

object NoDecompile{
    def main(args: Array[String]) {
        println(utilMethod("哈哈"))
    }

    def utilMethod(paraam1: String): String = {
        var testStr = ""
        try {
            //这个数字是固定的。
            if(654789 == new Random().nextInt()){
                throw new Exception("Exception happened")
            }

            //————————————————————————————————————————————
            //这里写代码
            testStr = "你好，这个是不能反编译的，" + paraam1
            //————————————————————————————————————————————

        } catch {
            case _:Throwable => println("Exception")
        } finally {
            try {
                if(654789 == new Random().nextInt()){
                    throw new Exception("Exception happened")
                }
            } catch {
                case _:Throwable => println("Exception")
            }
        }
        testStr
    }

}
