package sparkml

import utils.BaseUtil._
import utils.ConnectUtil

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/12/18
  * Time: 13:54 
  * Description: ML Pipelines提供了一套基于DataFrame构建的统一的高级API，可帮助用户创建和调整实用的机器学习流程。
  * 参考：http://spark.apache.org/docs/2.3.1/ml-pipeline.html
  */
object MLPipelines {
  val spark = ConnectUtil.spark

  import spark.implicits._

  /*
    DataFrame：此ML API使用Spark SQL中的DataFrame作为ML数据集，它可以包含各种数据类型。
    例如，DataFrame可以具有存储文本，特征向量，真实标签和预测的不同列
     */
  val 概念_DateFrame = 0

  /*
    管道组件
    1、Transformer是一种可以将一个DataFrame转换为另一个DataFrame的算法。
      例如，ML模型是变换器，其将具有特征的DataFrame转换为具有预测的DataFrame。
      特征变换器可以采用DataFrame，读取列（例如，文本），将其映射到新列（例如，特征向量），并输出附加有映射列的新DataFrame。
      学习模型可以采用DataFrame，读取包含特征向量的列，预测每个要素向量的标签，并输出新的DataFrame，其中预测标签作为列附加。
    2、Estimator是一种算法，可以适应DataFrame以生成Transformer。
      例如，学习算法是Estimator，其在DataFrame上训练并产生模型。
      Estimator抽象出 一种学习算法、拟合或训练数据的任何算法 的概念。
      从技术上讲，Estimator实现了一个方法fit（），它接受一个DataFrame并生成一个Model，它是一个Transformer。
      例如，诸如LogisticRegression之类的学习算法是Estimator，并且调用fit()训练LogisticRegressionModel，
        LogisticRegressionModel是Model，因此是Transformer。

    管道组件的属性：
      Transformer.transform()和Estimator.fit()都是无状态的。将来，可以通过替代概念支持有状态算法。
      Transformer或Estimator的每个实例都有一个唯一的ID，用于指定参数（如下所述）。


     */
  val 概念_管道组件 = 0

  /*
    Pipeline：
      在机器学习中，通常运行一系列算法来处理和学习数据。例如，简单的文本文档处理工作流程可能包括几个阶段：
        将每个文档的文本拆分为单词。
        将每个文档的单词转换为数字特征向量。
        使用特征向量和标签学习预测模型。
      MLlib将此类工作流表示为管道，其由一系列以特定顺序运行的PipelineStages（变换器和估算器）组成。我们将在本节中将此简单工作流用作运行示例。

    如何工作：
      http://spark.apache.org/docs/2.3.1/ml-pipeline.html#how-it-works
        管道被指定为阶段序列，并且每个阶段是Transformer或Estimator。
        这些阶段按顺序运行，输入的DataFrame在通过每个阶段时进行转换。
        对于Transformer阶段，在DataFrame上调用transform()方法。
        对于Estimator阶段，调用fit()方法以生成Transformer（它成为PipelineModel或拟合管道的一部分），并在DataFrame上调用Transformer的transform()方法。

        我们为简单的文本文档工作流说明了这一点。下图是管道的训练时间使用情况。
        http://spark.apache.org/docs/2.3.1/img/ml-Pipeline.png
        上图中，顶行表示具有三个阶段的管道。
          前两个（Tokenizer和HashingTF）是Transformers（蓝色），第三个（LogisticRegression）是Estimator（红色）。
        底行表示流经管道的数据，其中柱面表示DataFrame。
          在原始DataFrame上调用Pipeline.fit()方法，该原始DataFrame具有原始文本文档和标签。
          Tokenizer.transform()方法将原始文本文档拆分为单词，向DataFrame添加包含单词的新列。
          HashingTF.transform()方法将单词列转换为要素向量，将带有这些向量的新列添加到DataFrame。
          现在，由于LogisticRegression是一个Estimator，因此Pipeline首先调用LogisticRegression.fit()来生成LogisticRegressionModel。
          如果Pipeline有更多的Estimators，它会在将DataFrame传递给下一个阶段之前调用DataFrame上的LogisticRegressionModel的transform()方法。

        管道是估算器。因此，在Pipeline的fit()方法运行之后，它会生成一个PipelineModel，它是一个Transformer。
        这个PipelineModel在测试时使用;下图说明了这种用法。
          http://spark.apache.org/docs/2.3.1/img/ml-PipelineModel.png
          在上图中，PipelineModel具有与原始Pipeline相同的阶段数，但原始Pipeline中的所有Estimators都变为Transformers。
          在测试数据集上调用PipelineModel的transform()方法时，数据将按顺序传递到拟合的管道中。
          每个阶段的transform()方法更新数据集并将其传递到下一个阶段。

    细节
      http://spark.apache.org/docs/2.3.1/ml-pipeline.html#details
        DAG管道：管道的阶段被指定为有序数组。这里给出的例子都是线性管道，即其中每个阶段使用前一阶段产生的数据。
        只要数据流图形成有向无环图（DAG），就可以创建非线性管道。
        目前，此图基于每个阶段的输入和输出列名称（通常指定为参数）隐式指定。如果管道形成DAG，则必须按拓扑顺序指定阶段。

        运行时检查：由于Pipelines可以在具有不同类型的DataFrame上运行，因此它们不能使用编译时类型检查。
        Pipelines和PipelineModels代替在实际运行Pipeline之前进行运行时检查。
        此类型检查是使用DataFrame架构完成的，DataFrame架构是DataFrame中列的数据类型的描述。

        独特的管道阶段：管道的阶段应该是唯一的实例。例如，由于Pipeline阶段必须具有唯一ID，因此不应将同一实例myHashingTF插入管道两次。
        但是，不同的实例myHashingTF1和myHashingTF2（两者都是HashingTF类型）可以放在同一个管道中，因为将使用不同的ID创建不同的实例。
     */
  val 详细_管道 = 0

  /*
  MLlib Estimators和Transformers使用统一的API来指定参数。
  Param是一个带有自包含文档的命名参数。 ParamMap是一组（参数，值）对。
  将参数传递给算法有两种主要方法：
    1、设置实例的参数。例如，如果lr是LogisticRegression的实例，则可以调用lr.setMaxIter(10)以使lr.fit()最多使用10次迭代。
      此API类似于spark.mllib包中使用的API。
    2、将ParamMap传递给fit()或transform()。
      ParamMap中的任何参数都将覆盖先前通过setter方法指定的参数。参数属于Estimators和Transformers的特定实例。
      例如，如果我们有两个LogisticRegression实例lr1和lr2，那么我们可以构建一个指定了两个maxIter参数的ParamMap：
        ParamMap(lr1.maxIter -> 10，lr2.maxIter -> 20)。如果管道中有两个带有maxIter参数的算法，这将非常有用。
   */
  val 概念_参数 = 0

  /*
  通常，将模型或管道保存到磁盘以供以后使用是值得的。
    在Spark 1.6中，模型导入/导出功能已添加到Pipeline API中。
    从Spark 2.3开始，spark.ml和pyspark.ml中基于DataFrame的API具有完整的覆盖范围。
  ML持久性适用于Scala，Java和Python。但是，R当前使用的是修改后的格式，因此保存在R中的模型只能加载回R;这应该在将来修复，并在SPARK-15572中进行跟踪。

  ML持久性的向后兼容性
    通常，MLlib保持ML持久性的向后兼容性。
      即：如果您在一个版本的Spark中保存ML模型或Pipeline，那么您应该能够将其加载回来并在将来的Spark版本中使用它。
    但是，有少数例外情况，如下所述。
      模型持久性：Spark版本Y可以加载Spark版本X中使用Apache Spark ML持久性保存模型或管道吗？
        主要版本：无保证，但尽力而为。
        次要和补丁版本：是的;这些是向后兼容的。
        关于格式的注意事项：不保证稳定的持久性格式，但模型加载本身设计为向后兼容。
      模型行为：Spark版本X中的模型或管道在Spark版本Y中的行为是否相同？
        主要版本：无保证，但尽力而为。
        次要和补丁版本：相同的行为，除了错误修复。
    对于模型持久性和模型行为，在Spark版本发行说明中报告了次要版本或修补程序版本的任何重大更改。如果发行说明中未报告破损，则应将其视为要修复的错误。
   */
  val ML持久化 = 0

  val 例子_01 = 0
  //Estimator，Transformer和Param
  if (0) {
    // 从（标签，功能）元组列表中准备训练数据。
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

    // 创建LogisticRegression实例。这个实例是一个Estimator。
    val lr = new LogisticRegression()
    // 打印出参数，文档和任何默认值。
    println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")

    // 我们可以使用setter方法设置参数。
    lr.setMaxIter(10).setRegParam(0.01)

    // 了解LogisticRegression模型。使用存储在lr中的参数。
    val model1 = lr.fit(training)
    // 由于model1是Model（即Estimator生成的Transformer），我们可以查看fit()期间使用的参数。
    // 这将打印参数（name：value）对，其中names是此LogisticRegression实例的唯一ID
    println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")

    // 我们也可以使用ParamMap(支持多种指定参数的方法)指定参数
    val paramMap = ParamMap(lr.maxIter -> 20)
        .put(lr.maxIter, 30) // Specify 1 Param. This overwrites the original maxIter.
        .put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.

    // 也可以组合ParamMaps
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    // 现在使用paramMapCombined参数学习一个新模型。paramMapCombined通过lr.set*方法覆盖之前设置的所有参数。
    val model2 = lr.fit(training, paramMapCombined)
    println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")

    // 准备测试数据。
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    // 使用Transformer.transform()方法对测试数据进行预测。 LogisticRegression.transform仅使用“features”列。
    // 注意，model2.transform()输出'myProbability'列而不是通常的'probability'列，因为我们先前重命名了lr.probabilityCol参数。
    model2.transform(test)
        .select("features", "label", "myProbability", "prediction")
        .collect()
        .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
          println(s"($features, $label) -> prob=$prob, prediction=$prediction")
        }
  }

  val 例子02 = 0
  //http://spark.apache.org/docs/2.3.1/ml-pipeline.html#example-pipeline
  if (0) {
    // 从（id，text，label）元组列表中准备训练文档。
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // 配置ML管道，它由三个阶段组成：tokenizer，hashingTF和lr。
    val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
    val hashingTF = new HashingTF()
        .setNumFeatures(1000)
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
    val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.001)
    val pipeline = new Pipeline()
        .setStages(Array(tokenizer, hashingTF, lr))

    // 使管道适合训练文档。
    val model = pipeline.fit(training)

    // 现在我们可以选择将拟合的管道保存到磁盘
    model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // 我们还可以将这个未拟合的管道保存到磁盘
    pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    // 准备测试文档，这些文档是未标记的（id，text）元组。
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // 对测试文档进行预测。
    model.transform(test)
        .select("id", "text", "probability", "prediction")
        .collect()
        .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
          println(s"($id, $text) --> prob=$prob, prediction=$prediction")
        }
  }

}