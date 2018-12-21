package sparkml

import utils.BaseUtil._
import utils.ConnectUtil

import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.Row

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/12/18
  * Time: 11:37 
  * Description:基础统计
  */
object BasicStatistics extends App {
  //

  /*
  参考：http://spark.apache.org/docs/2.3.1/ml-statistics.html
  集群启动：cd /opt/spark-2.4.0-bin-hadoop2.7/bin
            ./spark-shell
   */
  val spark = ConnectUtil.spark

  import spark.implicits._

  /*
     Correlation使用指定的方法计算Vector的输入数据集的相关矩阵。输出将是一个DataFrame，它包含向量列的相关矩阵。
    */
  val 关联Correlation = 0
  if (0) {
    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val df = data.map(Tuple1.apply).toDF("features")
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")
  }

  /*
  假设检验是统计学中一种强有力的工具，用于确定结果是否具有统计显着性，无论该结果是否偶然发生。
   spark.ml目前支持Pearson的Chi-squared（χ^2）独立性测试。

  ChiSquareTest针对标签对每个功能进行Pearson独立测试。对于每个特征，（特征，标签）对被转换为应急矩阵，对其计算卡方统计量。
    所有标签和特征值必须是分类的。
   */
  val 假设检验 = 0
  if (1) {
    val data = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )

    val df = data.toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
  }

}