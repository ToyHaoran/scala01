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
object ExceptionDemo extends App{
    if (1) {
        try {
            val f = new FileReader("input.txt")
        } catch {
            case ex: FileNotFoundException =>
                println("Missing file exception")

            case ex: IOException =>
                println("IO Exception")
                ex.printStackTrace()

        } finally {
            println("Exiting finally...")
        }
    }
}