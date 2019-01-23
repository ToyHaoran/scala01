package scalademo.generic_type_demo

import utils.BaseUtil.int2boolen

/**
  * User: lihaoran 
  * Date: 2019/1/21
  * Time: 13:24 
  * Description: 对应快学scala17章
  */
object GenericTypeDemo extends App {
  /*
  泛型用于指定方法或类可以接受任意类型参数，参数在实际使用时才被确定，
  泛型可以有效地增强程序的适用性，使用泛型可以使得类或方法具有更强的通用性。
  泛型的典型应用场景是集合及集合中的方法参数，可以说同java一样，scala中泛型无处不在，具体可以查看scala的api
   */
  val 泛型类 = 0
  if (0) {
    class Pair[T, S](val first: T, val second: S)

    val p = new Pair(42, "String") //scala会从构造参数推断出实际类型，这很省心
    val p2 = new Pair[Int, Int](42, 34) //自己指定类型
  }

  val 泛型函数 = 0
  if (0) {
    //得到数组中间的元素 方法
    def getMiddle[T](a: Array[T]) = a(a.length / 2)
    val f = getMiddle[Int] _ //具体的函数
    val arr = Array(12, 23, 35)
    println(getMiddle(arr))
    println(f(arr))
  }

  val 类型变量界定_上界 = 0
  if (0) {
    /*
     我们并不知道first是否有compareTo方法。要解决这个问题，我们可以添加一个上界 T <: Comparable[T]
     这里T必须是Comparable[T]的子类型。这样我们就可以实例化Pair[java.lang.String]，但是不能实例化Pair[java.io.File]
     */
    class Pair[T <: Comparable[T]](val first: T, val second: T) {
      //得到较小的那个值
      def smaller = if (first.compareTo(second) < 0) first else second
    }

    val p = new Pair("Fred", "Brooks")
    println(p.smaller)

    //val p3 = new Pair(1,3)  //报错，见下面视图界定
    //println(p3.smaller)
  }

  val 类型变量界定_下界 = 0
  if (0) {
    val stu1 = new Student(89).setName("zhangsan")
    val stu2 = new Student(70).setName("lisi")
    val person = new Person

    val p2 = new Pair[Student](stu1, stu2)
    val p3 = p2.replaceFirst(person)
    println(p3.first.name)
  }

  val 视图界定 = 0
  if (0) {
    class Pair[T <% Comparable[T]](val first: T, val second: T) {
      def smaller = if (first.compareTo(second) < 0) first else second
    }

    val p = new Pair(12, 50)
    println(p.smaller)
  }

  val 上下文界定 = 0
  if (0) {
    // 视图界定 T <% V 要求必须存在一个从T到V的隐式转换，
    // 上下文界定的形式为 T:M，其中M是另一个泛型类。它要求必须存在一个类型为M[T]的“隐式值”。例如：
    class Pair[T: Ordering](val first: T, val second: T) {
      def smaller(implicit ord: Ordering[T]) = {
        if (ord.compare(first, second) < 0) first else second
      }
    }

    val p = new Pair(12, 50)
    println(p.smaller)
  }

  val Manifest上下文界定 = 0
  if (0) {
    /*
    要实例化一个泛型的Array[T]，我们需要一个Manifest[T]对象。
    如果你要编写一个泛型函数来构造泛型数组的话，你需要传入这个Manifest对象来帮忙。
    由于它是构造器的隐式参数，可以用上下文界定：
     */
    def makePair[T: Manifest](first: T, second: T) = {
      val r = new Array[T](2)
      r(0) = first
      r(1) = second
      r
    }
    val arr = makePair(4, 9)
    arr.foreach(println(_))
  }

  val 多重界定 = 0
  if (0) {
    /*
      类型变量可以同时有上界和下界。写法为：
      T >: Lower <: Upper

      你不能同时有多个上界或多个下界。不过你可以要求一个类型实现多个特质：
      T <: Comparable[T] with Serializable with Cloneable

      你也可以有多个视图界定：
      T <% Comparable[T] <% String

      你也可以有多个上下文界定:
      T : Ordering : Manifest

     */
  }

  val 类型约束 = 0
  if (0) {
    /*
    类型约束提供给你的是另一个限定类型的方式。总共有三种关系可供使用：
    T =:= U   // T是否等于U
    T <:< U   // T是否为U的子类型
    T <%< U  // T能否被视图(隐士)转换为U

    要使用这样一个约束，你需要添加“隐式类型证明参数”：
    class Pair[T](val first: T, val second: T) (implicit ev: T <:< Comparable[T])

    类型约束让你可以在泛型类中定义只能在特定条件下使用的方法。例如：
    */
    class Pair[T](val first: T, val second: T) {
      def smaller(implicit ev: T <:< Ordered[T]) = {
        if (first < second) first else second
      }
    }
    /*
    在这里你可以构造出Pair[File], 尽管File并不是带有先后次序的。只有当你调用smaller方法时，才会报错。
    另一个示例是Option类的orNull方法：
    val friends = Map("Fred" -> "Barney", ...)
    val friendOpt = friends.get("Wilma")
    val friendOrNull = friendOpt.orNull  // 要么是String，要么是null

    这种做法并不适用于值类型，例如Int。因为orNUll实现带有约束Null <:< A，
    你仍然可以实例化Option[Int]，只要你别使用orNull就好了。
    类型约束的另一个用途是改进类型推断。例如：
    def firstLast[A, C <: Iterable[A]](it: C) = (it.head, it.last)

    当你执行如下代码：
    firstLast(List(1,2,3))

    你会得到一个消息，推断出的类型参数[Nothing, List[Int]]不符合[A, C <: Iterable[A]]。
    类型推断器单凭List(1,2,3)无法判断出A是什么，因为它在同一个步骤中匹配到A和C。
    解决的方法是首先匹配C,然后在匹配A：
    def firstLast[A, C] (it: C) (implicit ev: C <:< Iterable[A]) = (it.head, it.last)
     */
  }

  val 型变 = 0
  if(0){
    /*
    假定我们有一个函数对pair[Person]做某种处理：
    def makeFriends(p: Pair[Person])

    如果Student是Person的子类，那么可以用Pair[Student]作为参数调用吗？缺省情况下，这是个错误。尽管Student是Person的子类型，但Pair[Student]和Pair[Person]之间没有任何关系。
    如果你想要这样的关系，则必须在定义Pair类时表明这一点：
    class Pair[+T] (val first: T, val second: T)

    +号意味着该类型是与T协变的-----也就是说，它与T按痛样的方向型变。由于Student是Person的子类，Pair[Student]也就是Pair[Person]的子类型。
    也可以有另一个方向的型变--逆变。例如：泛型Friend[T]，表示希望与类型T的人成为朋友的人。
    trait Friend[-T] {
      def befriend(someone: T)
    }

    // 有这么一个函数
    def makeFriendWith(s: Student, f: Friend[Student]) { f.befriend(s) }

    class Person extends Friend[Person]
    class Student extends Person
    val s = new Student
    val p = new Person

    函数调用makeFriendWith(s, p)能成功吗？这是可以的。
    这里类型变化 的方向和子类型方向是相反的。Student是Person的子类，但是Friend[Student]是Friend[Person]的超类。这就是逆变。
    在一个泛型的类型声明中，你可以同时使用这两中型变，例如 单参数函数的类型为Function1[-A, +R]
    def friends(students: Array[Atudent], find: Function1[Student, Person]) = {
      for (s <- students) yield find(s)
    }
     */
  }

  val 协变和逆变点 = 0
  if(0){
    /*
    协变和逆变点
    从上面可以看出函数在参数上是逆变的，在返回值上则是协变的。通常而言，对于某个对象消费的值适用逆变，而对于产出它的值则适用于协变。 如果一个对象同时是消费和产出值，则类型应该保持不变，这通常适用于可变数据结构。
    如果试着声明一个协变的可变对偶，则是错误的，例如：
    class Pair[+T] (var first: T, var second: T)  // 错误

    不过有时这也会妨碍我们做一些本来没有风险的事情。例如：
    class Pair[+T] (var first: T, var second: T) {
      def replaceFirst(newFirst: T) = new Pair[T](newFirst, second)  // error  T出现在了逆变点
    }

    // 解决方法是给方法加上另一个类型参数：
    class Pair[+T] (var first: T, var second: T) {
      def replaceFirst[R >: T](newFirst: R) = new Pair[R](newFirst, second)
    }

     */
  }

  val 类型通配符 = 0
  if(0){
    /*
    在Java中，所有泛型类都是不变的。不过，你可以在使用通配符改变它们的类型。例如：
    void makeFriends(Pair<? extends Person> people)   // Java代码

    可以用List<Student>作为参数调用。
    你也可以在Scala中使用通配符
    def process(people: java.util.List[_ <: Person])  // scala

    在Scala中，对于协变的Pair类，不需要通配符。但是假定Pair是不变的：
    class Pair[T] (var first: T, var second: T)

    // 可以定义函数：
    def makeFriends (p: Pair[_ <: Person])  // 可以用Pair[Student]调用

    逆变使用通配符：
    import java.util.Comparator
    def min[T](p: Pair[T]) (comp: Comparator[_ >: T])


     */
  }
}

class Person {
  var name: String = "lihaoran"
  var age: Int = 20
}

class Student extends Person {
  var grade: Long = _

  def this(grade: Long) {
    this()
    this.grade = grade
  }

  def setName(name: String): this.type = {
    this.name = name
    this
  }
}

//必须拿在最外面，否则报错
class Pair[T](val first: T, val second: T) {
  /*
     假定我们有一个Pair[Student]，我们应该允许使用一个Person来替换第一个组件。
     这样做的结果将会是一个Pair[Person]。通常而言，替换进来的类型必须是原类型的超类型。

     注意：如果不写下界：该方法可以编译通过，但是它将返回Pair[Any].
     */
  def replaceFirst[R >: T](newFirst: R) = new Pair(newFirst, second)
}