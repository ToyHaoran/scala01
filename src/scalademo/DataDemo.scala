package scalademo

import utils.BaseUtil.int2boolen

object DataDemo {

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
    if(0){
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
        n1.setScale(0)//报错，原因同下；会返回新的值，而不是修改原来的。
        n1.setScale(0,BigDecimal.RoundingMode.UNNECESSARY)//增加scale，减小报错。
        n1.setScale(0,BigDecimal.RoundingMode.HALF_UP)
        n1.setScale(0,BigDecimal.RoundingMode.CEILING)
        n1.setScale(0,BigDecimal.RoundingMode.FLOOR)
    }
}
