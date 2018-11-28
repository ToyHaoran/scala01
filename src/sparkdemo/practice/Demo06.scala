package sparkdemo.practice

import java.net.URL

import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import utils.ConnectUtil

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/20
  * Time: 9:18 
  * Description:
  * 利用spark的缓存机制，读取需要筛选的数据，
  * 自定义一个分区器，将不同的学科数据分别放到一个分区器中，
  * 并且根据指定的学科，取出点击量前三的数据，并写入文件。
  */
object Demo06 {
    def main(args: Array[String]) {
        //从数据库中加载规则
        //val arr = Array("java.learn.com", "php.learn.com", "net.learn.com")

        val sc = ConnectUtil.sc

        //获取数据
        //val file = sc.textFile("H:\\code\\idea\\scala01\\src\\sparkdemo\\testfile\\learn.log")
        val file = sc.textFile("src/sparkdemo/testfile/learn.log")

        //提取出url并生成一个元祖，rdd1将数据切分，元组中放的是（URL， 1）
        val urlAndOne = file.map(line => {
            val fields = line.split("\t")
            val url = fields(1)
            (url, 1)
        })

        //把相同的url进行聚合
        val sumedUrl = urlAndOne.reduceByKey(_ + _)

        //获取学科信息缓存,提高运行效率
        val cachedProject = sumedUrl.map(x => {
            val url = x._1
            val project = new URL(url).getHost
            // scala> new URL("http://java.learn.com/java/course/javaeeadvanced.shtml").getHost
            // res1: String = java.learn.com
            val count = x._2
            (project, (url, count))
        }).cache()

        //调用Spark自带的分区器此时会发生哈希碰撞，会有数据倾斜问题产生，需要自定义分区器
        //val res2 = cachedProject.partitionBy(new HashPartitioner(3))
        //res.saveAsTextFile("src/temp/Demo06out")

        //得到所有学科(确实要distinct，前面聚合的时候是url，而不是project)
        //就三个projects: Array[String] = Array(net.learn.com, php.learn.com, java.learn.com)
        val projects = cachedProject.keys.distinct().collect()

        //调用自定义分区器并得到分区号
        val partitioner = new ProjectPartitioner(projects)
        //val numPartitions = partitioner.numPartitions
        //val num = partitioner.getPartition("net.learn.com") //0

        //分区
        val partitioned: RDD[(String, (String, Int))] = cachedProject.partitionBy(partitioner)

        //对每个分区的数据进行排序并取top3
        val res = partitioned.mapPartitions(it => {
            it.toList.sortBy(_._2._2).reverse.take(3).iterator
        })
        res.saveAsTextFile("src/temp/Demo06out")

        sc.stop()
    }

    class ProjectPartitioner(projects: Array[String]) extends Partitioner {
        //用来存放学科和分区号
        private val projectsAndPartNum = new mutable.HashMap[String, Int]()
        //计数器，用于指定分区号
        var n = 0

        for (pro <- projects) {
            projectsAndPartNum += (pro -> n)
            n += 1
        }

        //得到分区数
        override def numPartitions = projects.length

        //得到分区号
        override def getPartition(key: Any) = {
            projectsAndPartNum.getOrElse(key.toString, 0)
        }
    }


}