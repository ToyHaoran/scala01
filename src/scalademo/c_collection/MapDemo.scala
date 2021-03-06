package scalademo.c_collection

import scala.collection.immutable.ListMap

object MapDemo extends App {
  //键值对
  val 不可变映射 = 0
  if (false) {
    val map = Map("i" -> 1, "j" -> 2)
    val map2 = Map(("i", 1), ("j", 2))
    map + ("k" -> 3)
    map - "j"
    //合并集合
    map ++ List("m" -> 2, "l" -> 7) //scala.collection.immutable.Map[String,Int] = Map(i -> 1, j -> 2, m -> 2, l -> 7)
    map -- List("m", "l")
    map.size
    map.contains("i")
    map.keys // Iterable[String] = Set(i, j)
    map.values //Iterable[Int] = MapLike(1, 2)
    map.keySet //scala.collection.immutable.Set[String] = Set(i, j)
    map.isEmpty

    //排序方式1
    val mapSortSmall = map.toList.sortBy(_._2) // 从小到大(默认)
    mapSortSmall.foreach(line => println(line._1 + "\t" + line._2))
    val mapSortBig = map.toList.sortBy(-_._2) // 从大到小
    //排序方式2
    val map3 = ListMap(map.toSeq.sortBy(_._2): _*)
    val map4 = ListMap(map.toSeq.sortWith(_._2 < _._2): _*)


    //有序的映射
    import scala.collection.immutable.TreeMap
    val tm = TreeMap(3 -> "x", 4 -> "y", 5 -> "z")
    tm + (4 -> "g") // scala.collection.immutable.TreeMap[Int,String] = Map(3 -> x, 4 -> g, 5 -> z)
  }


  val 可变映射 = 0
  if (false) {
    val words = collection.mutable.Map.empty[String, Int]
    words += ("one" -> 1)
    words.put("one", 10) //会覆盖前面的
    words -= "one"
    words ++= List("one" -> 1, "two" -> 2)
    words --= List("one", "two")
  }
}