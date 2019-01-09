package scalademo

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2019/1/4
  * Time: 13:39 
  * Description: 用来展示注释的写法。
  */
object DocDemo {
  /**
    * 文档注释,用来演示相关的一些语法。<br>
    * <p>p表示分段,会自动换行并空出一行<br>
    * br表示换行<br>
    * <b>这里是详细说明,加粗</b>
    * <p>第二段<br>
    * <h1>一级标题</h1>
    * <h2>二级标题</h2>
    * <ol>
    * <li>列表1</li>
    * <li>列表2</li>
    * <ul>
    * <li>列表1</li>
    * <li>列表2</li>
    * </ul>
    * </ol>
    * <code>
    * val str = "你好"
    * val str2 = str.toString
    * </code>
    *
    * @param str 传入的字符串，比如[[str01]]
    * @return 返回的字符串
    */
  def test(str: String): String = {
    println(str)
    str
  }

  val str01 = ""


}