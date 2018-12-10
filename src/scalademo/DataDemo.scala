package scalademo

import utils.BaseUtil._

object DataDemo extends App {

    val 基类及泛型 = 0
    if (1) {
        //Any是所有其他类的超类
        val temp1: Any = "123"
        val temp2: Any = List(1, 2, 3, 4)
        //AnyRef类是Scala里所有引用类(reference class)的基类
        val temp3: AnyRef = List(1, 2, 3, 4)

        //泛型
        def getxxx(a: Any): Unit = {
            println(a.toString)
        }
        getxxx(temp1)
        getxxx("444")

        def printMap(map: Map[_ <: Any, _ <: Any]): Unit = {
            for ((key, value) <- map) {
                println(key.toString + " : " + value.toString)
            }
            println("===================")
        }

    }

    val 常见数据类型 = 0
    /*
    数据类型	        描述
    Byte	        8位有符号补码整数。数值区间为 -128 到 127
    Short	        16位有符号补码整数。数值区间为 -32768 到 32767
    Int	            32位有符号补码整数。数值区间为 -2147483648 到 2147483647
    Long	        64位有符号补码整数。数值区间为 -9223372036854775808 到 9223372036854775807
    Float	        32 位, IEEE 754 标准的单精度浮点数
    Double	        64 位 IEEE 754 标准的双精度浮点数
    Char	        16位无符号Unicode字符, 区间值为 U+0000 到 U+FFFF
    String	        字符序列
    Boolean	        true或false
    Unit	        表示无值，和其他语言中void等同。用作不返回任何结果的方法的结果类型。Unit只有一个实例值，写成()。
    Null	        null 或空引用
    Nothing	        Nothing类型在Scala的类层级的最低端；它是任何其他类型的子类型。
    Any             Any是所有其他类的超类
    AnyRef	        AnyRef类是Scala里所有引用类(reference class)的基类
    上表中列出的数据类型都是对象，也就是说scala没有java中的原生类型。在scala是可以对数字等基础类型调用方法的
     */
    if (0) {
        val long01 = 35L
        val float01 = 3.1415f
        val char01 = 'a'

        val str01: String = "001"
        val str02 = "002" //类型可以推断
        val str03, str04 = "004"
    }


    val BigDecimalDemo = 0
    if (0) {
        //————————————————————————————BigDecimal的应用
        //问题1：
        val a = 1.01
        val b = 2.02
        val c = a + b
        println(c + " " + (c == 3.03)) //3.0300000000000002 false
        //问题2：
        println(1.1f == 1.1) //false
        getTypeName(1.1) //默认Double
        getTypeName(2) //默认Integer

        //创建方式（一般传入String作为参数）
        val n1 = BigDecimal(1.01)
        val n2 = BigDecimal.apply(2.02D)
        val n3 = BigDecimal("23.34423534534532")

        //类型装换
        n1.doubleValue()
        n1.toDouble
        n1.intValue()
        n1.toInt

        //重载了各种运算符
        val res1 = n1.+(n2)
        val res2 = n1 - n2
        val res3 = n1 * n2
        val res4 = n1 / n2
        val (division, remainder) = (n2 + BigDecimal(0.01D)) /% n1

        //比较两个大小数
        println(n1.compare(n2)) //-1
        println(n2.compare(n1)) //1
        println(n2.compare(n2)) //0 相等
        println(n1 > n2) //false
        println(n1 >= n2)

        //精度
        n1.precision
        n3.precision
        BigDecimal(123.4567D).precision //7 精度，总位数
        BigDecimal(123.4567D).scale //4 小数位数

        //保留小数位数
        /*
        输入数字	UP远离0	 DOWN靠近0	CEILING向上	FLOOR向下	HALF_UP远离0的舍入	HALF_DOWN靠近0的舍入	HALF_EVEN向相邻的偶数舍入	UNNECESSARY
        5.5	     6	        5	        6	        5	        6	                5	                    6	                异常
        2.5	     3	        2	        3	        2	        3	                2	                    2	                异常
        1.6	     2	        1	        2	        1	        2	                2	                    2	                异常
        1.1	     2	        1	        2	        1	        1	                1	                    1	                异常
        1.0	     1	        1	        1	        1	        1	                1	                    1	                1
        -1.0	-1	        -1	        -1	        -1	        -1	                -1	                    -1	                -1
        -1.1	-2	        -1	        -1	        -2	        -1	                -1	                    -1	                异常
        -1.6	-2	        -1	        -1	        -2	        -2	                -2	                    -2	                异常
        -2.5	-3	        -2	        -2	        -3	        -3	                -2	                    -2	                异常
        -5.5	-6	        -5	        -5	        -6	        -6	                -5	                    -6	                异常
         */
        val n4 = BigDecimal(1.123456789)
        //n4.setScale(0) //报错，原因同下；会返回新的值，而不是修改原来的。
        //n4.setScale(0, BigDecimal.RoundingMode.UNNECESSARY) //增加scale，减小会导致报错。
        n4.setScale(0, BigDecimal.RoundingMode.HALF_UP)
        n4.setScale(0, BigDecimal.RoundingMode.CEILING)
        n4.setScale(0, BigDecimal.RoundingMode.FLOOR)


    }
}

object StringDemo extends App {

    val String的常用方法 = 0
    if (0) {
        val str = "hello"
        println(str.charAt(1))
        println(str.compareTo("hallo"))
        println(str.compareToIgnoreCase("Hello"))
        println(str.concat(" world"))
        println(str.contentEquals(new StringBuilder("hello")))
        println(String.copyValueOf(Array('a', 'b', 'c')))
        println(str.endsWith("lo"))
        println("-----------")
        println(str.equals("hall"))
        println(str.equalsIgnoreCase("HELLO"))
        println(str.getBytes().mkString(" "))
        println(str.getBytes("utf-8").mkString(" "))
        val str2 = new Array[Char](10)
        println(str.getChars(0, str.length-1, str2, 1))
        println(str.hashCode)
        println(str.indexOf("l"))
        println(str.indexOf("l", 3))
        println(str.lastIndexOf("o"))
        println(str.lastIndexOf("lo",-1))
        println("---------------")
        println(str.intern())
        println(str.matches("[a-z]+"))
        println(str.replaceAll("he","kkkkk"))
        println(str.replaceFirst("l","op"))
        println(str.startsWith("he"))
        println(str.split("l"))
        println(str.substring(2,4))
        println(str.toCharArray.mkString(" "))
        println(str.toLowerCase)
        println("=================")
        println(str.toUpperCase)
        println(str.trim)
        println(getTypeName(String.valueOf(4645))) //以字符串形式返回
        println(String.valueOf(4645.454d))
    }

    val stringBuilder的优势 = 0
    if (0) {
        val time1 = getMethodRunTime({
            var str1 = ""
            for (i <- 1 to 10000) {
                str1 += "append"
            }
            str1
        })._2

        val time2 = getMethodRunTime({
            val str2 = new StringBuilder()
            for (i <- 1 to 10000) {
                str2.append("append")
            }
            str2.toString()
        })._2
        print(time1 + " " + time2) //1.160s 0.004s
    }

    val stringBuilder的常用方法 = 0
    if(1){
        val str = new StringBuilder()
        str.append("hello ").append(7878).append(" ").append(3.1415d)
        println(str.toString())
        println(str.insert(1," world ").toString())
        println(str.delete(1, 5).toString()) //在上面的基础上删除
        println(str.setCharAt(5,'H'))
        println(str.replace(5,6,"oooooo"))
        println(str.result())
        println(str.reverseContents())
        println(str.reverse)
        println(str)
        println(str.indexOf("8787"))
    }


}