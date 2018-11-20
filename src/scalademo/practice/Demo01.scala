package scalademo.practice

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/20
  * Time: 8:59 
  * Description:
  */
object Demo01 {
    //  1、用Scala实现选猴王的算法。又叫约瑟夫问题
    //  15个猴子围成一圈选大王，一次1-7循环报数，报到7的猴子淘汰，直到最后一只猴子成为大王，程序输出猴王编号。
    //思路：无

    //普通算法
    def monkeyKing2(n: Int, m: Int): Int = {
        var z = 0 //猴王标记

        //初始化，把所有的猴子先标记为1
        val a = new Array[Int](n) //定长数组
        for (i <- 0 to a.length - 1) {
            a(i) = 1
        }
        var leftCount = n //剩余猴子的数量
        var countNum = 0 //目前数到了第几个
        var index = 0 //定义当前的位置从0开始。

        //如果当前点的左边数量不为1的话
        //把踢出的猴子标记改为0，未被踢出的不变，依然为1
        while (leftCount != 1) {

            if (a(index) == 1) {
                //如果当前剩余猴子的数量大于1，然后标记还为1，那么就在计数器中加1
                countNum += 1
                //计数器的数和设定被踢出的猴子的数目相同的时候，踢出猴子，把标记改为0
                if (countNum == m) {
                    countNum = 0 //刷新计数器，初始化为0
                    a(index) = 0 //改变当前的标记为0
                    leftCount -= 1 //在剩余的猴子里面减一
                }
            }
            index += 1 //改变当前的位置
            //改变现有猴子的长度
            //当走到了末尾，转到第一个位置
            if (index == a.length) {
                index = 0
            }
        }

        //从第一个到最后一个查找哪个的标记是0,如果是0的话说明被踢出，如果是1的话，则为剩余的猴王
        for (i <- 0 to a.length - 1) {
            if (a(i) == 1) {
                z = i
                System.out.println("第" + (i + 1) + "只猴子被选为大王!")
            }
        }
        z+1
    }

    /**
      * 精简算法.(看不懂)
      * 由于猴子是围成一圈的，所以实际序号是可以无限大的，我们不妨假设当前序号z是前面一次序号减去了m后剩下的序号，
      * 如果前面一次序号小于m，只要反复+j（当前面一次有j个猴子时），让序号大于m即可，
      * 而最后剩下一个猴子的时候序号肯定为0，返回值是++z正好为1，代表第一个猴子。
      * 由此可见，只要每次将当期序号z反过来加上m后，就是前面一次的序号了，除j取余只不过是为了让序号正好在0~(j-1)的范围内而已。
      *
      * @param n a个猴子
      * @param m 被T出的猴子是第几个
      * @return 猴王的序号
      */
    def monkeyKing(n:Int, m:Int): Int={
        var z=0 //z是王
        for(i <- 2 to n) z=(z+m)%i
        z+1
    }



    println(monkeyKing(15,7))
    println(monkeyKing2(15,7))
}