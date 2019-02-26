package utils

/**
  * User: lihaoran 
  * Date: 2019/2/22
  * Time: 10:54 
  * Description: 用来解析各种东西
  */
object ParseUtil {
  val email = "w+([-+.]w+)*@w+([-.]w+)*.w+([-.]w+)* ".r

  /**
    * 从SQL语句中解析出来表名,只能用于一层的SQL
    *
    * @return 表名
    */
  def sql2Table(sql: String): String = {
    //使用正则表达式解析表名
    //val matcher = "[fF][rR][oO][mM] [a-zA-Z0-9_]+".r
    //或者使用管道(|)来设置不同的模式
    //val matcher = "(from|FROM) ([a-zA-Z0-9_]+)".r
    //或者整体忽略大小写
    val matcher = "(?i)from [A-Z0-9_]+".r
    matcher.findAllIn(sql).toList.head.split(" ").toList.last
  }


}