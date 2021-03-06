package scalademo.b_function

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{File, FileInputStream, PrintWriter}
import java.net.URL
import scala.io.Source

object IODemo extends App {
  val 读取控制台输入 = 0
  if (false) {
    import scala.io.StdIn._
    val name = readLine("Your name:") //Enter键结束
    print("Your age:")
    val age = readInt()
    println(name + "   " + age)
  }

  val 读取文件 = 0
  if (false) {
    //——————————————————————读取————————————————————————————————————
    //读取文件
    //  val source = Source.fromFile("G:\\Practice\\Scala\\source\\myfile.txt","UTF-8")//也可以
    //  val source = Source.fromFile("src/source/hello.txt", "UTF-8")
    val source = Source.fromFile("G:/person.txt", "UTF-8")
    //绝对路径
    val source3 = Source.fromString("hello world")
    val source4 = Source.fromBytes("hello world".getBytes)
    //同上
    val source5 = Source.stdin
    //从标准输入读取，CTRL+D结束     //命令行好像没法操作。
    val source6 = Source.fromURL(new URL("http://hadoop.apache.org/docs/r2.7.1/")) //读取整个网页html代码

    source.getLines().foreach(println(_)) //读取行
    source.foreach(println(_)) //读取单个字符
    source.mkString //将整个文件读成一个字符串

    source.close()
    //输出一次就自动关闭。
  }
  val 读取输入流 = 0
  if (false) {
    //————————————————————————读取输入流——————————————————————————————————
    //读取二进制文件(scala没有提供，需要使用java类库)
    val file = new File("g:/person.txt")
    val in = new FileInputStream(file)
    val byte = new Array[Byte](file.length().toInt)
    in.read(byte)
    in.close()
    //写入
    val out = new PrintWriter("g:/number.txt")
    for (i <- 1 to 100) out.println(i)
    out.close()
  }

  val 读取HDFS文件 = 0
  if (false) {
    //读取HDFS文件并输出
    val config = new Configuration()
    config.set("fs.default.name", "hdfs://localhost:9000")
    config.set("dfs.replication", "1")
    val fs = FileSystem.get(config)
    val inputStream = fs.open(new Path("/user/wcinput/person2.txt"))
    val source2 = Source.fromInputStream(inputStream)
    //输出
    source2.getLines().foreach(println(_)) //输出一次就关闭了
    source2.mkString
  }

}
