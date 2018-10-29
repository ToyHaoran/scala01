package utils

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/10/29
  * Time: 14:52 
  * Description:
  */
object BaseUtil {
    /**
      * 用来快捷控制代码的开关，将0，1转化为false和true
      * @param a
      * @return
      */
    implicit def int2boolen(a: Int): Boolean = if (a == 0) false else true

}