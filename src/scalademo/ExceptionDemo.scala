package scalademo

import java.io.{FileNotFoundException, FileReader, IOException}

import utils.BaseUtil._

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/13
  * Time: 15:03 
  * Description:
  */
object ExceptionDemo extends App {
  if (1) {
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException =>
        println("Missing file exception===============")
        println("1 " + ex.getCause)  //null
        println("2 " + ex.getMessage)  // 2 input.txt (系统找不到指定的文件。)
        println("3 " + ex.getLocalizedMessage)  //同上
        println("4 " + ex.getStackTrace.mkString("\n"))  //同下
        ex.printStackTrace()

      case ex: IOException =>
        println("IO Exception")
        ex.printStackTrace()

    } finally {
      println("无论怎么样，最终都要退出=========")
    }
  }
}