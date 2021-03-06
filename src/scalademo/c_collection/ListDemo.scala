package scalademo.c_collection

//Scala的Seq将是Java的List，Scala的List将是Java的LinkedList。
object ListDemo extends App {
  val 不可变列表List = 0
  if(false){
    val list1 = List(1, 2, 3, 4)
    println("对数组元素进行折叠======")
    (10 /: list1) (_ + _) //对数组中所有的元素进行相同的操作 ，foldLeft的简写
    (10 /: list1) (_ * _)
    (list1 :\ 10) (_ + _) //foldRight的简写

    println("将集合转为字符串===")
    //等价于mkString，不过一个是StringBuilder，一个是String
    list1.addString(new StringBuilder)
    //本质是调用的上面的方法
    list1.mkString("[", "-", "]")

    list1.par.aggregate(5)(_ + _, _ + _) //聚合，类似RDD的方法
    list1.canEqual(List(5, 6, 7, 8)) //判断两个对象是否能比较

    list1.contains(2)
    list1.exists(_ < 2)
    list1.endsWith(Array(3, 4))
    list1.containsSlice(List(2, 3)) //是否包含子集
    list1.containsSlice(List(4, 3)) //false

    list1.count(_ > 2) //统计符合条件的元素个数，下面统计大于 2 的元素个数
    list1.distinct //去重
    list1.diff(List(4, 5, 6, 7)) //计算当前数组与另一个数组的不同。将当前数组中没有在另一个数组中出现的元素返回

    //判断两个序列长度以及对应位置元素是否符合某个条件。如果两个序列具有相同的元素数量并且p(x, y)=true，返回结果为true
    //下面代码检查a和b长度是否相等，并且a中元素是否小于b中对应位置的元素
    list1.corresponds(List(5, 6, 7, 8))(_ < _)
  }

  if(false){
    println("创建列表===========")
    // (注意列表是不可变的，只能用::来添加元素)
    val site: List[String] = List("Runoob", "Google", "Baidu")
    val site1 = "Runoob" :: ("Google" :: ("Baidu" :: Nil)) //构造列表，底层代码，::右结合
    val nums: List[Int] = List(1, 2, 3, 4)
    val nums2: List[Int] = List(5, 6, 7, 8)
    val empty: List[Nothing] = List()
    val dim: List[List[Int]] = List(List(1, 0, 0), List(0, 1, 0), List(0, 0, 1)) //二维列表
    //以下操作参考Array
    List.range(0, 10, 2)
    List.fill(10)("hello")
    List.iterate(2, 5)(a => a * 2) //List[Int] = List(2, 4, 8, 16, 32)
    List.tabulate(5)(a => a * 2) // List[Int] = List(0, 2, 4, 6, 8)
    List.tabulate(2, 3)(_ + _) //下标相加： List[List[Int]] = List(List(0, 1, 2), List(1, 2, 3))

    //列表的基本操作

    //添加元素到头部
    5 +: nums //同下
    5 :: nums //同下
    nums.+:(5) // List[Int] = List(5, 1, 2, 3, 4)  注意是放到前面的,而且不能去掉.号，也不能去掉()
    nums.::(5)
    nums.+:(5, 6) // List[Any] = List((5,6), 1, 2, 3, 4)  注意是一个元素
    nums.::(5, 6) //类似于+:在开头添加元素，但是::可以用于pattern match ，而+:则不行
    //添加元素到尾部，产生新集合
    nums.:+(5) // List[Int] = List(1, 2, 3, 4, 5) 注意是放到后面的
    nums :+ 5 //可以去掉.号和()   同上
    nums :+ (5) //同上
    //连接两个集合
    site ++ site1
    //连接两个列表(以下等价)
    site ::: site1
    site.:::(site1)
    site ::: nums //List[Any] = List(Runoob, Google, Baidu, 1, 2, 3, 4)
    List.concat(site, site1)

    //模式匹配：求和
    def sum(lst: List[Int]): Int = {
      lst match {
        case Nil => 0
        //::用于pattern match，在Scala中列表要么为空（Nil表示空列表）要么是一个head元素加上一个tail列表
        case h :: t => h + sum(t) //h是lst.head，而t是lst.tail
      }
    }
    nums.sum //简化方法


    //获取列表的元素（只能作用于非空列表）
    site.head //返回第一个元素     String = Runoob
    site.tail //返回除了第一个元素的列表    List[String] = List(Google, Baidu)
    site.tail.tail.tail //Nil

    //与head和tail不同之处是：head和tail运行时间是常量，但init和last需要遍历整个列表，时间较长。
    site.last //返回最后一个元素
    site.init //返回除了最后一个元素的列表

    site.take(2) //返回前两个元素
    site.takeRight(2) //返回后两个元素

    site.drop(2) //扔掉前两个元素

    site.splitAt(1) //在下标1处切分，返回pair列表 (List[String], List[String]) = (List(Runoob),List(Google, Baidu))

    site.indices //返回列表的索引值组成的序列：  scala.collection.immutable.Range = Range(0, 1, 2)
    //咬合操作
    site.indices.toList.zip(site) //List[(Int, String)] = List((0,Runoob), (1,Google), (2,Baidu))
    site.zipWithIndex // List[(String, Int)] = List((Runoob,0), (Google,1), (Baidu,2))

    //显示字符串
    site.toString() // String = List(Runoob, Google, Baidu)
    site.mkString(",") //String = Runoob,Google,Baidu
    site.mkString(">>>", "—", "<<<") //String = >>>Runoob—Google—Baidu<<<

    //连续存放的数组和递归存放的列表之间进行转换
    site.toArray
    site.toArray.toList
    val arr = new Array[String](10)
    site.copyToArray(arr, 3) //从下标3开始

    //枚举器访问元素列表
    val it = site.iterator //Iterator[String] = non-empty iterator
    it.next()
    it.next()
    it.hasNext

    site.isEmpty //判断是否为空（不要用site.length==0）
    site.length //列表长度（遍历整个列表，比较费时）
    site.reverse //反转列表

    //高级操作
    //列表间映射
    nums.map(x => x + 1)
    site.map(_.length)
    site.map(_.toList.reverse.mkString) //List[String] = List(boonuR, elgooG, udiaB)
    site.flatMap(_.toList) // List[Char] = List(R, u, n, o, o, b, G, o, o, g, l, e, B, a, i, d, u)
    site.foreach(println(_))
    //列表过滤
    nums.filter(_ + 1 < 4) //符合条件的过滤出来   List[Int] = List(1, 2)
    nums.find(_ + 1 < 4) // 返回第一个满足的元素，后不满足，返回None   Option[Int] = Some(1)
    nums.partition(_ % 2 == 0) //满足条件的在前，不满足的在后    (List[Int], List[Int]) = (List(2, 4),List(1, 3))
    //以下没有filter好用，因为只是获取前缀，一旦在后面就获取不到。
    site.takeWhile(str => str.length == 6) //返回满足条件的前缀，一旦不满足就返回空
    site.takeWhile(str => str.length == 5) //返回Nil，因为第一个就不满足，有Baidu也没用。
    site.dropWhile(str => str.length == 6) //扔掉满足条件的前缀
    site.dropWhile(str => str.startsWith("B")) //返回全部，Baidu是第三个，不会被扔掉
    site.span(_.length == 6) //将前两者结合     (List[String], List[String]) = (List(Runoob, Google),List(Baidu))
    //列表的论断
    site.forall(_.length >= 5) //所有元素必须全部满足才返回true
    site.exists(_.length == 5) //只要有一个元素满足就返回true

    //折叠操作(可以用来拼接join的连接条件,神级操作，见 sparkdemo.JoinDemo.scala:33)
    site.foldRight("lll")(_ + "--" + _) //String = Runoob--Google--Baidu--lll
    site.foldLeft("lll")(_ + "--" + _) // String = lll--Runoob--Google--Baidu

    nums.sum //求和    10
    nums.product //相乘    24

    //列表排序
    List(3, 2, 4, 5, 2, 5, 7).sorted //按大小排序
    List(3, 2, 4, 5, 2, 5, 7).sortWith(_ > _) //从大到小排序
    site.sortWith(_.length < _.length) //按长度排序
  }

  val 可变列表ListBuffer = 0
  if(false){
    import scala.collection.mutable.ListBuffer
    val buf = new ListBuffer[Int]
    buf += 1
    buf +=(2, 3) //buf.type = ListBuffer(1, 2, 3)
    (4 +: buf).toList
  }

}
