package utils

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran
  * Date: 2018/11/1
  * Description: 用来创造数据
  */
object DataCreateUtil{

    def main(args: Array[String]): Unit = {
        println(textCreate(999," ", lineSplitFlag = 2))
    }


    /**
      *
      * @param totalTimes 基础单词的总次数
      * @param sep 分隔符，默认","
      * @param lineSplitFlag 换行标记，0：不换行，1：固定次数换行，2：随机次数换行，默认0
      * @param perNum 如果固定换行，每行的单词数，默认10
      */
    def textCreate(totalTimes:Int, sep: String = ",",lineSplitFlag:Int = 0, perNum:Int = 10): String = {
        //用来造的基础数据，比如说水果，基本单词
        val fruitArray = Array("苹果", "梨", "橘子", "芭蕉", "草莓", "龙眼", "香蕉", "榴莲", "荔枝", "橙子", "橘子", "甘蔗",
            "枇杷", "葡萄", "芒果", "李子", "桃子", "提子", "哈密瓜", "香瓜", "木瓜", "火龙果", "猕猴桃", "雪梨", "西瓜", "石榴", "香梨",
            "山竹", "蟠桃", "贡梨", "鸭梨", "菠萝", "柚子", "樱桃", "椰子", "无花果", "山野葡萄", "桑葚", "人参果", "柿子", "杏子")

        val len = fruitArray.length
        //TODO 拆分方法
        if (totalTimes < 1000) {
            //用来随机换行的变量
            var ranCreateFlag = true
            var ranCreated = 0
            //用来保存数据的列表
            var lst = List[String]()
            for (i <- 1 to totalTimes) {
                // 基于索引的快速访问需要使用数组
                val ran = scala.util.Random.nextInt(len)
                val str = lineSplitFlag match {
                    case 0 =>
                        //不换行
                        fruitArray(ran)
                    case 1 =>
                        //固定次数换行，也可以用随机换行的方法
                        if (i % perNum == 0) {
                            //注意第一行少一个
                            "\n" + fruitArray(ran)
                        } else {
                            fruitArray(ran)
                        }
                    case 2 =>
                        // TODO 将换行符放在前面，然后mkString("")
                        /*思路：
                        随机5-10个就换行
                        弄个flag，运行一次弄个随机数，然后后面的几次就不产生随机数了
                        等到运行完指定的次数，然后再产生一个随机数
                         */
                        if(ranCreateFlag){
                            //这里设置4-9会产生5-10个数据(第一行会少一个)
                            ranCreated = scala.util.Random.nextInt(6) + 4
                            ranCreateFlag = false
                        }
                        if (ranCreated != 0){
                            ranCreated -= 1
                            fruitArray(ran)
                        }else{
                            //需要换行
                            ranCreateFlag = true
                            "\n" + fruitArray(ran)
                            //这里会导致第一行少一个，但是\n放在前面的一个的后面会导致mkString分隔符跑到前面去
                            //如果不嫌麻烦，可以自己拼接字符串，然后用mkString(sep="")
                        }
                }
                // 快速添加使用列表
                lst = str :: lst
            }
            lst.mkString(sep)
        }else{
            // TODO 并行计算
            "并行计算"
        }
    }


















}
