package demo

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2019/1/4
  * Time: 9:56 
  * Description:
  */
class Demo01 {
  var path:String = _

  def setName(path: String): this.type = {
    this.path = path
    this
  }

}

object xxx extends App{
  val demo = new Demo01
  println(demo.setName("hahaha").path)
}