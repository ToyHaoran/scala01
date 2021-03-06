package scalademo.a_data

import utils.BaseUtil.{getMethodRunTime, getTypeName}


object StringDemo extends App {

  val String的常用方法 = 0
  if (false) {
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
    println(str.getChars(0, str.length - 1, str2, 1))
    println(str.hashCode)
    println(str.indexOf("l"))
    println(str.indexOf("l", 3))
    println(str.lastIndexOf("o"))
    println(str.lastIndexOf("lo", -1))
    println("---------------")
    println(str.intern())
    println(str.matches("[a-z]+"))
    println(str.replaceAll("he", "kkkkk"))
    println(str.replaceFirst("l", "op"))
    println(str.startsWith("he"))
    println(str.split("l"))
    println(str.substring(2, 4))
    println(str.toCharArray.mkString(" "))
    println(str.toLowerCase)
    println("=================")
    println(str.toUpperCase)
    println(str.trim)
    println(getTypeName(String.valueOf(4645))) //以字符串形式返回
    println(String.valueOf(4645.454d))
  }

  val stringBuilder的优势 = 0
  if (false) {
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
  if (false) {
    val str = new StringBuilder()
    str.append("hello ").append(7878).append(" ").append(3.1415d)
    println(str.toString())
    println(str.insert(1, " world ").toString())
    println(str.delete(1, 5).toString()) //在上面的基础上删除
    println(str.setCharAt(5, 'H'))
    println(str.replace(5, 6, "oooooo"))
    println(str.result())
    println(str.reverseContents())
    println(str.reverse)
    println(str)
    println(str.indexOf("8787"))
  }


}
