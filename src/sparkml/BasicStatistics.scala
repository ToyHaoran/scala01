package sparkml

import utils.BaseUtil._
import utils.ConnectUtil
import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/12/18
  * Time: 11:37 
  * Description: Spark ML 基础
  */
object BasicStatistics extends App {

  /*
  参考：http://spark.apache.org/docs/2.3.1/ml-statistics.html
  集群启动：cd /opt/spark-2.4.0-bin-hadoop2.7/bin
            ./spark-shell
   */
  val spark = ConnectUtil.spark
  val sc = ConnectUtil.sc

  import spark.implicits._

  val _________数据类型RDD____________ = 0
  /*
  本地向量具有整数类型和基于0的索引和双类型值，存储在单个机器上。
  MLlib支持两种类型的本地向量：密集和稀疏。
    密集向量由表示其条目值的double数组支持，而稀疏向量由两个并行数组支持：索引和值。
    例如，矢量(1.0, 0.0, 3.0)可以以密集格式表示:[1.0, 0.0, 3.0]
    或者以稀疏格式表示(3, [0, 2], [1.0, 3.0])，其中3是矢量的大小。
   */
  val 本地矢量_LocalVectors = 0
  if (0) {
    //必须手动导入，否则会默认导入Scala的Vector
    import org.apache.spark.mllib.linalg.{Vector, Vectors}
    //创建密集向量
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    //创建稀疏向量
    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
  }

  /*
  标记点是与标签/响应相关联的密集或稀疏的本地矢量。
  在MLlib中，标记点用于监督学习算法。我们使用double来存储标签，因此我们可以在回归和分类中使用标记点。
  对于二进制分类，标签应为0（负）或1（正）。对于多类分类，标签应该是从零开始的类索引：0, 1, 2, ...。
   */
  val 标记点_Labeled_point = 0
  if (0) {
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint
    // Create a labeled point with a positive label and a dense feature vector.
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    println(pos.features)
    println(pos.label)

    // Create a labeled point with a negative label and a sparse feature vector.
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
  }

  /*
  在实践中很常见的是具有稀疏的训练数据。
  MLlib支持读取以LIBSVM格式存储的训练样例，格式是LIBSVM和使用的默认格式LIBLINEAR。
  它是一种文本格式，其中每一行使用以下格式表示标记的稀疏特征向量：
    label index1:value1 index2:value2 ...
   */
  val 读取libsvm数据 = 0
  if (0) {
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils
    import org.apache.spark.rdd.RDD

    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    examples.foreach(println)

    //或者用spark直接读取。
    val examples2 = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
  }

  /*
  http://spark.apache.org/docs/2.3.1/mllib-data-types.html#local-matrix
  本地矩阵具有整数类型的行和列索引以及双类型值，存储在单个机器上。
  MLlib支持密集矩阵，其条目值以列主要顺序存储在单个双数组中，
  以及稀疏矩阵，其非零条目值以列主要顺序存储在压缩稀疏列（CSC）格式中。
   */
  val 本地矩阵_LocalMatrix = 0
  if (0) {
    import org.apache.spark.mllib.linalg.{Matrix, Matrices}

    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    // 稀疏矩阵解释，3,2表示行列数
    // Array(0, 1, 3)：0表示开始迭代，1表示第一列非0元素个数，3表示第二列与第一列的非0元素个数累加和
    // https://blog.csdn.net/sinat_29508201/article/details/54089771
    // 或者每列非0个数：(0, 1, 3) => (1-0,3-1) => (1,2)
    // Array(0, 2, 1)表示非0元素分别对应的行号
    // Array(9, 6, 8)表示非0元素。
    // 此处设计比较好，假设100个元素分两列，不需要把每个元素所在列都标出来，只需要记录3个数字即可。
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    // (0, 3, 3, 4) => (3-0,3-3,4-3) => (3,0,1) 每列个数
    Matrices.sparse(3, 3, Array(0, 3, 3, 4), Array(0, 2, 1, 1), Array(9, 6, 8, 10))
    Matrices.sparse(3, 3, Array(0, 2, 3, 4), Array(0, 2, 1, 1), Array(9, 6, 8, 10))
    Matrices.sparse(3, 3, Array(1, 2, 3, 4), Array(0, 2, 1, 1), Array(9, 6, 8, 10))
    Matrices.sparse(3, 3, Array(2, 2, 3, 4), Array(0, 2, 1, 1), Array(9, 6, 8, 10))
  }

  /*
  分布式矩阵：
  一个分布式矩阵也是由下标和double型的值组成，不过分布式矩阵的下标不是Int型，而是long型，数据分布式保存在一个或多个RDD中。
  选择正确的格式来保存海量和分布式的矩阵是非常重要的。将分布式矩阵转换成不同的格式需要一个全局的shuffle(global shuffle)，
  而全局shuffle的代价会非常高。到目前为止，Spark MLlib中已经实现了4种分布式矩阵

　最基本的分布式矩阵是RowMatrix，它是一个行式的分布式矩阵，没有行索引。比如一系列特征向量的集合。
    RowMatrix由一个RDD代表所有的行，每一行是一个本地向量。
    假设一个RowMatrix的列数不是特别巨大，那么一个简单的本地向量能够与driver进行联系，并且数据可以在单个节点上保存或使用。
  IndexedRowMatrix与RowMatrix类似但是有行索引，行索引可以用来区分行并且进行连接等操作。
  CoordinateMatrix是一个以协同列表（coordinate list)格式存储数据的分布式矩阵，数据以RDD形式存储。
  BlockMatrix是由RDD支持的分布式矩阵，其RDD MatrixBlock 是元组(Int, Int, Matrix)。

　注意：因为我们需要缓存矩阵的大小，所以分布式矩阵的RDDs格式是需要确定的，使用非确定RDDs的话会报错。
   */
  val 分布式矩阵 = 0

  /*
    RowMatrix它是一个行式的分布式矩阵，没有行索引。比如一系列特征向量的集合。
      RowMatrix由一个RDD代表所有的行，每一行是一个本地向量。
      因为每一行代表一个本地向量，所以它的列数被限制在Integer.max的范围内，在实际应用中不会太大。
　　一个RowMatrix可以由一个RDD[Vector]的实例创建。因此我们可以计算统计信息或者进行分解。
    QR分解（QR decomposition）是A=QR，其中Q是一个矩阵，R是一个上三角矩阵。
    对sigular value decomposition(SVD)和principal component analysis（PCA）,可以去参考降维的部分。
   */
  val 分布式矩阵_RowMatrix = 0
  if (0) {
    import org.apache.spark.mllib.linalg.distributed.RowMatrix
    import org.apache.spark.mllib.linalg.{Vector, Vectors}

    val v0 = Vectors.dense(1.0, 0.0, 3.0)
    val v1 = Vectors.dense(3.0, 2.0, 4.0)
    val rows: RDD[Vector] = sc.parallelize(Seq(v0, v1)) // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)
    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    //获取统计摘要
    val summary = mat.computeColumnSummaryStatistics()
    //最大向量
    summary.max
    //以通过summary实例来获取矩阵的相关统计信息，例如行数
    summary.count
    //方差向量
    summary.variance
    //平均向量
    summary.mean
    //L1范数向量
    summary.normL1
    summary.normL2
    summary.numNonzeros


    // QR decomposition
    val qrResult = mat.tallSkinnyQR(true)
  }

  /*
  IndexedRowMatrix与RowMatrix类似，但是它有行索引。由一个行索引RDD表示，索引每一行由一个long型行索引和一个本地向量组成。
　一个IndexedRowMatrix可以由RDD[IndexedRow]的实例来生成，IndexedRow是一个（Long, Vector)的封装。
  去掉行索引，IndexedRowMatrix能够转换成RowMatrix。
   */
  val 分布式矩阵_IndexedRowMatrix = 0
  if (0) {
    import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
    import org.apache.spark.mllib.linalg.{Vector, Vectors}

    val dv1 = Vectors.dense(1.0, 0.0, 3.0)
    val dv2 = Vectors.dense(3.0, 2.0, 4.0)
    val idxr1 = IndexedRow(1, dv1)
    val idxr2 = IndexedRow(2, dv2)

    val rows: RDD[IndexedRow] = sc.parallelize(Array(idxr1, idxr2)) // an RDD of indexed rows
    // Create an IndexedRowMatrix from an RDD[IndexedRow].
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)
    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    mat.rows.foreach(println)

    // Drop its row indices.
    val rowMat: RowMatrix = mat.toRowMatrix()
  }

  /*
  http://spark.apache.org/docs/2.3.1/mllib-data-types.html#coordinatematrix
  坐标矩阵
  CoordinateMatrix是由其条目的RDD支持的分布式矩阵。
  每个条目都是一个元组(i: Long, j: Long, value: Double)，其中i是行索引，j是列索引， value是条目值。
  CoordinateMatrix只有当矩阵的两个维度都很大并且矩阵非常稀疏时，才应该使用
   */
  val 分布式矩阵_CoordinateMatrix = 0
  if (0) {
    import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

    val ent1 = new MatrixEntry(0, 1, 0.5)
    val ent2 = new MatrixEntry(2, 2, 1.8)
    val entries: RDD[MatrixEntry] = sc.parallelize(Array(ent1, ent2)) // an RDD of matrix entries
    // Create a CoordinateMatrix from an RDD[MatrixEntry].
    val mat: CoordinateMatrix = new CoordinateMatrix(entries)
    mat.entries.foreach(println)

    //转置
    mat.transpose()

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    // Convert it to an IndexRowMatrix whose rows are sparse vectors.
    val indexedRowMatrix = mat.toIndexedRowMatrix()
  }

  /*
  http://spark.apache.org/docs/2.3.1/mllib-data-types.html#blockmatrix
  BlockMatrix是通过的RDD支持的分布式矩阵MatrixBlocks，其中一个MatrixBlock是一个元组((Int, Int), Matrix)，
  其中，(Int, Int)是所述块的索引，并且Matrix是大小与所述给定索引处的子矩阵rowsPerBlockX colsPerBlock。
  BlockMatrix支持诸如add和multiply其他方法BlockMatrix。
  BlockMatrix还有一个辅助功能validate，可以用来检查是否 BlockMatrix正确设置。
   */
  val 分布式矩阵_BlockMatrix = 0
  if (0) {
    import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}

    val ent1 = new MatrixEntry(0, 0, 1)
    val ent2 = new MatrixEntry(1, 1, 1)
    val ent3 = new MatrixEntry(2, 0, -1)
    val ent4 = new MatrixEntry(2, 1, 2)
    val ent5 = new MatrixEntry(2, 2, 1)
    val ent6 = new MatrixEntry(3, 0, 1)
    val ent7 = new MatrixEntry(3, 1, 1)
    val ent8 = new MatrixEntry(3, 3, 1)

    val entries: RDD[MatrixEntry] = sc.parallelize(Array(ent1,ent2,ent3,ent4,ent5,ent6,ent7,ent8)) // an RDD of (i, j, v) matrix entries
    // Create a CoordinateMatrix from an RDD[MatrixEntry].
    val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)
    // 将坐标矩阵转换成2x2的分块矩阵并存储，尺寸通过参数传入
    val matA: BlockMatrix = coordMat.toBlockMatrix(2,2).cache()

    // 判断是否分块成功
    // Nothing happens if it is valid.
    matA.validate()

    matA.toLocalMatrix()
    matA.numColBlocks
    matA.numRowBlocks

    // Calculate A^T A.  计算矩阵A和其转置矩阵的积矩阵
    val ata = matA.transpose.multiply(matA)

    //详细见：https://blog.csdn.net/legotime/article/details/51089644
  }


  val _________基本统计__________ = 0
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