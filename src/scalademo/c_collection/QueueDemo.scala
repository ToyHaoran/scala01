package scalademo.c_collection

object QueueDemo extends App {
  println("先进后出的序列========")
  val 不可变序列 = 0
  if (false) {
    import scala.collection.immutable.Queue
    val que = Queue[Int]() //不能加new，不知道为什么
    val que2 = Queue(1, 4, 5, 2, 3, 4, 6)

    println("添加元素=======")
    que.enqueue(1).enqueue(List(2, 3, 4))
    //scala.collection.immutable.Queue[Int] = Queue(1, 2, 3, 4)


    println("从头部移除元素========")
    val (elemaen, que3) = que2.dequeue //返回对偶Tuple2
    //elemaen: Int = 1
    //que3: scala.collection.immutable.Queue[Int] = Queue(4, 5, 2, 3, 4, 6)

  }

  val 可变序列 = 0
  if (false) {
    val que4 = new scala.collection.mutable.Queue[String]()
    que4 += "a"
    que4 ++= List("b", "c", "d") //que4.type = Queue(a, b, c, d)
    que4.dequeue() // String = a
  }
}