package scalademo.c_collection

/**
  * 序列是继承自特质Seq的类，可以处理一组线性分布的数据。元素是有序的。
  * 如果已经确定数组大小，使用Array。如果后期需要对数组进行插入和删除，使用Arraybuffer。
  */
object ArrayDemo extends App {
  val array基本操作 = 0
  if(false) {
    val arr1 = Array("hello", "world")
    val arr2 = Array("你好", "世界")

    println("数组查找========")
    println(arr1(0)) //hello
    println(arr1.apply(0))

    println("数组中元素更新=========")
    arr1.update(0, "hello2")
    arr1(1) = "world2"

    println("数组插入=========")
    //在数组前面添加一个元素，并返回新的对象 666,hello2,world2
    println(("666" +: arr1).mkString(","))
    //与前面相反,在后面加个元素 hello2,world2,777
    println((arr1 :+ "777").mkString(","))
    println(arr1.mkString(","))

    println("数组删除======")
    arr1.drop(1) //返回新的数组，arr1并没有变。

    println("同类型数组合并====")
    //和List合并后，右边的类型决定结果类型
    arr1 ++ arr2 //Array(hello, world, 你好, 世界)
    Array.concat(arr1, arr2)
  }

  val array高级操作 = 0
  if(false){
    println("初始化数组=======")
    val nums = new Array[Int](10) //初始化为0
    val nums1 = new Array[String](10) //初始化为null
    val nums2 = Array(1, 2, 4, 3, 2, 4) //调用apply方法
    val nums3 = Array("hello", "world")
    println("返回长度为0的数组=====")
    Array.empty[Int]
    println("创建区间数组======")
    Array.range(0, 10) // 不包括最后一个，Array[Int] = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    Array.range(0, 10, 2) //间隔为2   Array[Int] = Array(0, 2, 4, 6, 8)
    println("填充数组======")
    //返回数组，长度为第一个参数指定，同时每个元素使用第二个参数进行填充。
    Array.fill(3)(math.random) // Array[Double] = Array(0.47204059555458355, 0.4619077488315301, 0.02781911462725528)
    Array.fill(2, 3)("11") // Array[Array[String]] = Array(Array(11, 11, 11), Array(11, 11, 11))
    println("利用函数创建数组======") //感觉可以用for循环构建。。
    //返回：start, f(start), f(f(start)), ... 初始值可以自定义
    Array.iterate(0, 3)(a => a + 5) //Array[Int] = Array(0, 5, 10)   初始值为 0，长度为 3，计算函数为a=>a+5:
    Array.iterate(2, 3)(a => a * 5) // Array[Int] = Array(2, 10, 50)
    //返回：f(0),f(1), ..., f(n - 1)       a可以当做下标。
    Array.tabulate(3)(a => a + 5) //Array[Int] = Array(5, 6, 7)
    Array.tabulate(3)(a => a * 5) //Array[Int] = Array(0, 5, 10)
    //多维的更复杂，不常用（可以看源码）
    Array.tabulate(3, 4)((a, b) => a + b) //下标相加
    Array.tabulate(3, 4)(_ + _) //Array[Array[Int]] = Array(Array(0, 1, 2, 3), Array(1, 2, 3, 4), Array(2, 3, 4, 5))

    println("复制数组=====")
    nums.clone() //克隆数组
    //复制一个数组到另一个数组：从num2的下标2开始复制3个到nums的下标4开始（注意不要越界）
    Array.copy(nums2, 2, nums, 4, 3)

    println("多维数组======")
    //创建规则三维数组,赋值1-27
    val x = Array.ofDim[Int](3, 3, 3)
    var idx = 1
    for (i <- x.indices) {
      for (j <- x(i).indices) {
        for (k <- x(i)(j).indices) {
          x(i)(j)(k) = idx
          idx += 1
        }
      }
    }
    println("遍历三维数组====")
    for (i <- x) {
      //x是三维数组，i是二维数组
      for (j <- i) {
        //j是一维数组，只有j可以用mkString
        println(j.mkString(" "))
        j.foreach(arg => print(arg * 10 + " "))
      }
    }
    //创建不规则多维数组
    val x2 = new Array[Array[Int]](10)
    for (i <- x2.indices) {
      x2(i) = new Array[Int](i + 1)
    }
  }

  val arrayBuffer基本操作 = 0
  if(false){
    import scala.collection.mutable.ArrayBuffer
    println("创建数组缓冲，增删改查操作=========")
    val b = ArrayBuffer[Int]()
    println("添加==")
    b += 1 //在末尾添加元素
    b +=(2, 3, 4, 5) //添加集合
    b ++= Array(6, 7, 8, 9) //必须用++=追加任何集合
    b.insert(2, 11) //在下标2位置添加11
    println("删除===")
    b.trimEnd(2) //移除后面2个元素，高效操作
    b.remove(5) //移除下标5
    b -= 7 //移除元素7(一个一个删除，从前往后)
    b.remove(3, 2) //从下标3开始移除2个

    println("过滤===")
    b.drop(2) //扔掉前2个元素
    b.dropRight(2) //后两个
    b.dropWhile(_ == 3) //丢弃条件所在集合开头的所有项目true。一旦第一项失败，它就会停止丢弃。
    b.filter(_ > 3) //丢弃整个集合中条件不正确的所有项目。它直到收集结束才停止。
    b.filterNot(_ > 3)

    println("查看和修改=====")
    b(1)
    b(2) = 3
    b.find(_ > 2) //查找第一个符合条件的元素

    println("与Array的相互转化===")
    ArrayBuffer(b.toArray: _*) //可以将任何集合通过这种方式转为想要的集合。

    println("常用算法***=====")
    b.sum //求和
    b.count(a => a + 3 == 7) //求满足条件的个数
    b.reverse //数组倒转
    b.sorted //数组排序
    println(b.mkString(","))

    val arr3 = Array('a', 'b', 'c')
    //排列组合，这个排列组合会选出所有包含字符不一样的组合，对于 “abc”、“cba”，只选择一个，参数n表示序列长度，就是几个字符为一组
    arr3.combinations(2).foreach((item) => println(item.mkString(",")))
    arr3.combinations(3).foreach((item) => println(item.mkString(",")))

    Array(1, 2, 3, 4).flatMap(x => 1 to x)
    Array(Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(1, 2, 3, 4)).flatten //将二维数组的所有元素联合在一起，形成一个一维数组返回
  }
}
