package sparkgraphx

import utils.BaseUtil._
import utils.ConnectUtil
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/12/25
  * Time: 9:31 
  * Description:
  */
object demo01 {
  val sc = ConnectUtil.sc

  val 介绍 = 0
  /*
  类成员
  在GraphX中，图的基础类为Garph，它包含两个RDD：一个为边RDD，另一个为顶点RDD。
    可以用给定的边RDD和顶点RDD构建一个图。一旦构建好图，就可以用edges()和vertices()来访问边和顶点的集合。
    VD和ED代表了用户自定义的顶点和边类，对应的图是参数化类型的泛类型Graph[VD,ED]。GraphX中图必须要有顶点和边属性。
    GraphX中Vertice和Edge持有VerticeId值，而不是顶点的引用。
    图在集群中是分布式存储的，不属于单个JVM，因此一条边的顶点可能在不同的集群节点上。

  顶点： Vertice(VertexId, VD)
      abstract class VertexRDD[VD] extends RDD[(VertexId, VD)]
    抽象值成员
      innerJoin  leftJoin mapValues  ···
    具体值成员
      collect count distinct filter foreach groupBy isEmpty persist map reduce sortBy toString  ···

   边：Edge(VertexId, VertexId, ED）   
    class Edge[ED](srcId:VertexId, dstId:VertexId, attire:E
    abstract class EdgeRDD[ED] extends RDD[Edge[ED]]
    抽象值成员
      innerJoin mapValues reverse
    具体值成员
      ++ aggregate cache collect count distinct filter foreach groupBy isEmpty map persist reduce sortBy toString ···

    class EdgeTriplet[VD, ED] extends Edge[ED]
    值成员
      Attr srcId srcAttr dstId dstAttr


    图：Graph(VD, ED)        
        abstract class Graph[VD,ED] extend Serializable
      抽象值成员
        cache edges mapEdges mapTriplets mapVertices mask outerJoinVertices persist  reverse subgraph triplets vertices ···
      具体值成员
        aggregateMessages mapEdges mapTriplets ···

      class GraphOps[VD,ED] extends Serializable
      值成员
        collectEdges collectNeiborIds collectNeibors degrees filter inDegrees joinVertices numEdges numVertices outDegrees pageRank personalizedPageRank pickRandomVertex pregel  triangleCount ···
   */

  val graph = {
    // 创建点RDD，vertices，点（user）
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(
      Seq((3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))))

    // 创建边RDD，edges  边（关系）
    val relationships: RDD[Edge[String]] = sc.parallelize(
      Seq(Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi")))
    // 定义一个默认用户，避免有不存在用户的关系
    val defaultUser = ("John Doe", "Missing")
    // 构造Graph
    val graph = Graph(users, relationships, defaultUser)
    graph
  }

  val 缓存 = 0
  graph.cache()
  if (0) {
    //缓存。默认情况下,缓存在内存的图会在内存紧张的时候被强制清理，采用的是LRU算法
    graph.cache()
    //graph.persist(StorageLevel.MEMORY_ONLY)
    //graph.unpersistVertices(true)
  }


  /*
   点、边和三元组
    下面的代码用到了Edge样本类。边有一个srcId和dstId分别对应于源和目标顶点的标示符。
       另外，Edge类有一个attr成员用来存储边属性。可以分别用graph.vertices和graph.edges成员将一个图解构为相应的顶点和边。
       graph.vertices返回一个VertexRDD[(String, String)]，它继承于 RDD[(VertexID, (String, String))]。
       所以我们可以用scala的case表达式解构这个元组。
       另一方面，graph.edges返回一个包含Edge[String]对象的EdgeRDD，我们也可以用到case类的类型构造器。
   除了属性图的顶点和边视图，GraphX也包含了一个三元组视图，
       三元视图逻辑上将顶点和边的属性保存为一个RDD[EdgeTriplet[VD, ED]]，它包含EdgeTriplet类的实例。
       EdgeTriplet类继承于Edge类，并且加入了srcAttr和dstAttr成员，这两个成员分别包含源和目的的属性。
       我们可以用一个三元组视图渲染字符串集合用来描述用户之间的关系。
    */
  val 点_边_三元组 = 0
  //参考此图：http://spark.apache.org/docs/2.3.1/img/property_graph.png
  if (0) {
    // 找出职业为postdoc的人
    graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.collect
    // 计算源顶点ID大于目标顶点ID的边的数量
    graph.edges.filter(e => e.srcId > e.dstId).count
    graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
    // 使用三元组视图描述关系事实
    val facts: RDD[String] = graph.triplets.map(triplet =>
      s"${triplet.srcAttr._1} is the ${triplet.attr} of ${triplet.dstAttr._1}")
    facts.collect.foreach(println(_))
    /*
    rxin is the collab of jgonzal
    franklin is the advisor of rxin
    istoica is the colleague of franklin
    franklin is the pi of jgonzal
     */
  }

  /*
  度、入度、出度（简而言之，就是一个点上有几个边，几个入边，几个出边）
    正如RDDs有基本的操作map, filter和reduceByKey一样，属性图也有基本的集合操作，这些操作采用用户自定义的函数并产生包含转换特征和结构的新图。
      定义在Graph中的核心操作是经过优化的实现。表示为核心操作的组合的便捷操作定义在GraphOps中。
      然而， 因为有Scala的隐式转换，定义在GraphOps中的操作可以作为Graph的成员自动使用。
      例如，我们可以通过下面的方式计算每个顶点(定义在GraphOps中)的入度。
      区分核心图操作和GraphOps的原因是为了在将来支持不同的图表示。
      每个图表示都必须提供核心操作的实现并重用很多定义在GraphOps中的有用操作。
   */
  val 度_入度_出度 = 0
  if (0) {
    val degrees: VertexRDD[Int] = graph.degrees
    degrees.collect().foreach(println)
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.collect().foreach(println)
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.collect().foreach(println)
  }

  /*
  属性操作：修改顶点和边的属性
    属性操作每个操作都产生一个新的图，这个新的图包含通过用户自定义的map操作修改后的顶点或边的属性。
      Map操作根据原图的一些特性得到新图，原图结构是不变的。这些操作的一个重要特征是它允许所得图形重用原有图形的结构索引(indices)。
      下面的两行代码在逻辑上是等价的，但是第一个不是图操作，它不保存结构索引，所以不会从GraphX系统优化中受益。
      Map操作根据原图的一些特性得到新图，原图结构是不变的。这些操作经常用来初始化的图形，用作特定计算或者用来处理项目不需要的属性。
      例如，给定一个图，这个图的顶点特征包含出度，我们为PageRank初始化它。
  */
  val 属性操作_map = 0
  if (0) {
    //顶点转换，顶点age+1
    //RDD操作，再构造新图，不保存结构索引，不会被系统优化
    val newVertices = graph.vertices.map { case (id, attr) =>
      (id, (attr._1 + "-1", attr._2 + "-2"))
    }
    val newGraph1 = Graph(newVertices, graph.edges)
    newGraph1.triplets.collect().foreach(println)

    //图Map操作，被系统优化
    val newGraph2 = graph.mapVertices((id, attr) => (id, (attr._1 + "-1", attr._2 + "-2")))
    newGraph2.triplets.collect().foreach(println)
  }

  val 属性操作_join = 0
  if (0) {
    //构造一个新图，顶点属性是出度
    val inputGraph: Graph[Int, String] = graph.outerJoinVertices(
      graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    //根据顶点属性为出度的图构造一个新图，依据PageRank算法初始化边与点
    val outputGraph: Graph[Double, Double] = inputGraph.mapTriplets(
      triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
  }

  val 属性操作_自定义类型 = 0
  if (0) {
    //创建一个新图，顶点 VD 的数据类型为 User，并从 graph 做类型转换
    case class User(name: String, pos: String, inDeg: Int, outDeg: Int)
    val initialUserGraph: Graph[User, String] = graph.mapVertices {
      case (id, (name, pos)) => User(name, pos, 0, 0)
    }
    //initialUserGraph 与 inDegrees、outDegrees（RDD）进行连接，并修改 initialUserGraph中 inDeg 值、outDeg 值
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) =>
        User(u.name, u.pos, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) =>
        User(u.name, u.pos, u.inDeg, outDegOpt.getOrElse(0))
    }
    userGraph.vertices.collect.foreach(v =>
      println(s"${v._2.name} inDeg: ${v._2.inDeg} outDeg: ${v._2.outDeg}")
    )
    //出度和入读相同的人员
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }
  }

}