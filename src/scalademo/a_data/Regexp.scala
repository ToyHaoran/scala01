package scalademo.a_data

object Regexp extends App {
  val 在线工具 = 0
  if (false) {
    /*
    http://tool.oschina.net/regex#
     */
  }

  //正则表达式
  if (false) {
    val numPattern = "[0-9]+".r //等价于：Pattern.compile("...........")
    val wsnumwsPattern =
      """\s+[0-9]+\s+""".r
    //返回遍历所有匹配项的迭代器
    for (matchString <- numPattern.findAllIn("99 bottles, 98 bottles")) {
      println(matchString)
    }

    //找到首个匹配项
    val m1 = wsnumwsPattern.findFirstIn("99 bottles, 98 bottles")
    println(m1)

    //类似于startwith，从头开始匹配
    numPattern.findPrefixOf("99 bottles, 98 bottles")

    //替换
    numPattern.replaceFirstIn("99 bottles, 98 bottles", "xx")
    numPattern.replaceAllIn("99 bottles, 98 bottles", "xx")
  }

  if (false) {
    print("正则表达式组========")
    val numitemPattern = "([0-9]+) ([a-z]+)".r
    for (numitemPattern(num, item) <- numitemPattern.findAllIn("99 bottles, 98 bottles")) {
      println(num + " " + item)
    }
  }

  val 不区分大小写 = 0
}
