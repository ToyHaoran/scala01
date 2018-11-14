package scalademo

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/14
  * Time: 16:05 
  * Description: 主要介绍小括号和花括号的使用
  */
object ParenthesesDemo extends App{

    //人们会笼统地说在函数调用时，小括号和花括号是通用的，但实际上，情况会复杂的多

    /*
    总之：
    大括号{}用于代码块，计算结果是代码最后一行；
    大括号{}用于拥有代码块的函数；
    大括号{}在只有一行代码时可以省略，除了case语句（Scala适用）；
    小括号()在函数只有一个参数时可以省略（Scala适用）；
    几乎没有二者都省略的情况。
    参考foreach--case的括号
     */

    if(true){
        println("如果你要调用的函数有两个或两个以上的参数，那么你只能使用“小括号”====")
        val add = (x: Int,y: Int) => x + y
        add(1,2)
        //add{1,2} 报错

        println("如果你要调用的函数只有单一参数，那么“通常”情况下小括号和花括号是可以互换的===")
        val increase = (x: Int) => x + 1
        increase(10)
        increase{10}

        println("在调用单一参数函数时，小括号和花括号虽然等效，但还是有差异的.===")
        // 如果使用小括号，意味着你告诉编译器：它只接受单一的一行，因此，如果你意外地输入2行或更多，编译器就会报错。
        // 但对花括号来说则不然，如果你在花括号里忘记了一个操作符，代码是可以编辑的，
        // 但是会得到出乎意料的结果，进而导致难以追踪的Bug. 看如下的例子：
        def method(x: Int) = {
            x + 1
        }
        //以下代码可不报错，但是一编译就Waring
        /*def method1(x: Int) = {
            1+
            3
            5
        }*/
        /*
        //报错，1+2是一个完整的表达式，表示1行，3表示另一行
        def method2(x:Int) = (
            1+
            2
            3
            )*/

        println("在调用一个单一参数函数时，如果参数本身是一个通过case语句实现的 “偏函数”，你只能使用“花括号”")
        // 究其原因，我觉得scala对小括号和花括号的使用还是有一种“习惯上”的定位的：
        // 通常人们还是会认为小括号是面向单行的，花括号面向多行的。
        // 在使用case实现偏函数时，通常都会是多个case语句，小括号不能满足这种场合，
        // 只是说在只有一个case语句时，会让人产生误解，认为只有一行，为什么不使用case语句。
        val tupleList = List[(String, String)]()
        //val filtered = tupleList.takeWhile( case (s1, s2) => s1 == s2 ) //报错
        val filtered = tupleList.takeWhile{ case (s1, s2) => s1 == s2 }

        println("作为表达式（expression）和语句块（code blocks）时====")
        // 在非函数调用时，小括号可以用于界定表达式，花括号可以用于界定代码块。
        // 代码块由多条语句(statement)组成，每一条语句可以是一个”import”语句，一个变量或函数的声明，或者是一个表达式（expression），
        // 而一个表达式必定也是一条语句（statement），所以小括号可能出现在花括号里面，
        // 同时，语句块里可以出现表达式，所以花括号也可能出现在小括号里。看看下面的例子：
        /*
        1       // literal - 字面量
        (1)     // expression - 表达式
        {1}     // block of code - 代码块
        ({1})   // expression with a block of code - 表达式里是一个语句块
        {(1)}   // block of code with an expression - 语句块里是一个表达式
        ({(1)}) // you get the drift... - 你懂的。。。
         */
    }


}