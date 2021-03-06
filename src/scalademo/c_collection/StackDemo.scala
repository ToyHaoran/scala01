package scalademo.c_collection

object StackDemo extends App {
  println("先进先出的序列===========")
  if (true) {
    import scala.collection.mutable.Stack
    val stack = new Stack[Int]
    stack.push(1).push(2, 3, 4) //元素的推入    stack.type = Stack(4, 3, 2, 1)
    stack.pop //元素的弹出    Int = 4
    stack.top //只获取，不移除
  }
}