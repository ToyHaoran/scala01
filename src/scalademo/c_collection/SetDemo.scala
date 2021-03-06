package scalademo.c_collection

object SetDemo extends App {
  println("集没有重复值=========")

  val 不可变集 = 0
  if (false) {
    val set = Set(1, 1, 2, 3, 4, 5, 5) //scala.collection.immutable.Set[Int] = Set(5, 1, 2, 3, 4)
    val set2 = Set("aaa", "aaa", "dfg", "abc") // scala.collection.immutable.Set[String] = Set(aaa, dfg, abc)
    val set3 = set + 5 //添加元素
    set - 3 //删除元素
    set ++ List(5, 7, 8) //scala.collection.immutable.Set[Int] = Set(5, 1, 2, 7, 3, 8, 4)
    set -- List(1, 2, 3)
    set -- List(1, 2, 9) //scala.collection.immutable.Set[Int] = Set(5, 3, 4)
    set.size
    set.contains(5)

    set.head
    set.tail
    set.isEmpty
    set.++(set2)
    set ++ set2

    set.min
    set.max
    set.&(Set(3, 4, 7, 8)) //取交集 3，4
    set.intersect(Set(3, 4, 7, 8))
    //以上方法都是traversableonce的方法，集合通用的

    //有序的集,具体的顺序取决于Order特质
    import scala.collection.immutable.TreeSet
    val ts = TreeSet(34, 5, 5, 6, 3, 3, 5, 6, 4, 3, 3, 6, 7, 8, 1) // scala.collection.immutable.TreeSet[Int] = TreeSet(1, 3, 4, 5, 6, 7, 8, 34)
    val cs = TreeSet('f', 'u', 'n') //scala.collection.immutable.TreeSet[Char] = TreeSet(f, n, u)
  }

  val 可变集 = 0
  if (false) {
    val words = scala.collection.mutable.Set.empty[String]
    words += "the"
    words -= "the"
    words ++= List("do", "re", "mi")
    words --= List("do", "re")
    words.clear() //删除所有元素
  }
}