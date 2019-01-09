package sparkml

import utils.BaseUtil._


/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/12/20
  * Time: 13:57 
  * Description:分类和回归：本页面包含分类和回归的算法。它还包括讨论特定算法类的部分，例如线性方法，树和集合。
  */
object ClassificationAndRegression extends App {
  val _______________分类________________ = 0
  /*
  http://spark.apache.org/docs/2.3.1/ml-classification-regression.html#logistic-regression
  逻辑回归是预测分类响应的常用方法。广义线性模型的一个特例是预测结果的概率。
    在spark.ml中，逻辑回归可以用于通过二项逻辑回归来预测二元结果，或者它可以用于通过使用多项逻辑回归来预测多类结果。
    使用family参数在这两个算法之间进行选择，或者保持不设置，Spark将推断出正确的变量。
  通过将族参数设置为“多项式”，可以将多项逻辑回归用于二元分类。它将产生两组系数和两个截距。
  当在不具有常量非零列的数据集上截断LogisticRegressionModel时，Spark MLlib为常量非零列输出零系数。此行为与R glmnet相同，但与LIBSVM不同。
   */
  val 二项逻辑回归_Binomial_logistic_regression = 0
  if (0) {
    import org.apache.spark.ml.classification.LogisticRegression

    // Load training data
    val training = spark.read.format("libsvm")
        .load("data/mllib/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // We can also use the multinomial family for binary classification
    val mlr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
        .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    // Print the coefficients and intercepts for logistic regression with multinomial family
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
  }

  val 多项逻辑回归 = 0

  val 决策树分类器 = 0

  val 随机森林分类器 = 0

  val 梯度提升树分类器 = 0

  val 多层感知器分类器 = 0

  val 线性支持向量机 = 0

  val One_vs_Rest分类器 = 0

  val 朴素贝叶斯 = 0

  val _______________回归________________ = 0

  /*
  API：http://spark.apache.org/docs/2.3.1/api/scala/index.html#org.apache.spark.ml.regression.LinearRegression
   */
  val 线性回归_Linear_regression = 0
  if (0) {
    import org.apache.spark.ml.regression.LinearRegression
    // 加载训练数据
    val training = spark.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
    val lr = new LinearRegression()
        .setMaxIter(10)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
    // Fit the model
    val lrModel = lr.fit(training)
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
  }

  val 广义线性回归_Generalized_linear_regression = 0
  if (0) {
    import org.apache.spark.ml.regression.GeneralizedLinearRegression

    // Load training data
    val dataset = spark.read.format("libsvm")
        .load("data/mllib/sample_linear_regression_data.txt")

    val glr = new GeneralizedLinearRegression()
        .setFamily("gaussian")
        .setLink("identity")
        .setMaxIter(10)
        .setRegParam(0.3)

    // Fit the model
    val model = glr.fit(dataset)

    // Print the coefficients and intercept for generalized linear regression model
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    val summary = model.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals().show()
  }

  val 决策树回归 = 0

  val 随机森林回归 = 0

  val 梯度提升树回归 = 0

  val 生存回归 = 0

  val 等渗回归 = 0


  val _______________线性方法____________ = 0

  val _______________决策树______________ = 0

  val _______________树组合______________ = 0

  val 随机森林 = 0

  val 梯度树 = 0
}