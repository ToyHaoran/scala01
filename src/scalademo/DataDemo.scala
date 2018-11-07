package scalademo

import utils.BaseUtil.int2boolen

object DataDemo extends App {

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
    if (1) {
        val long01 = 35L
        val float01 = 3.1415f
        val char01 = 'a'

        val str01: String = "001"
        val str02 = "002" //类型可以推断
        val str03, str04 = "004"
    }


    val BigDecimalDemo = 0
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
    if (0) {
        //————————————————————————————BigDecimal的应用
        /* 问题：
        scala> 1.01+2.02
        res0: Double = 3.0300000000000002
        解决方式：BigDecimal
         */

        //创建方式（一般传入String作为参数）
        val n1 = BigDecimal(1.01)
        val n2 = BigDecimal.apply(2.02D)
        val n3 = BigDecimal("23.34423534534532")


        //重载了各种运算符
        n1.+(n2)
        n1 - n2
        n1 * n2
        n1 / n2
        n1 > n2


        //保留小数位数
        n1.setScale(0) //报错，原因同下；会返回新的值，而不是修改原来的。
        n1.setScale(0, BigDecimal.RoundingMode.UNNECESSARY) //增加scale，减小报错。
        n1.setScale(0, BigDecimal.RoundingMode.HALF_UP)
        n1.setScale(0, BigDecimal.RoundingMode.CEILING)
        n1.setScale(0, BigDecimal.RoundingMode.FLOOR)

        //比较两个大小数
        println(n1.compare(n2))
    }
}
