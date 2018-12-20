package sparkml

import utils.BaseUtil._
import utils.ConnectUtil

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/12/19
  * Time: 13:33 
  * Description:
  */
object ExtractingAndTransforming extends App {
  val spark = ConnectUtil.spark

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#tf-idf

  词频－逆向文件频率（TF-IDF）是一种在文本挖掘中广泛使用的特征向量化方法，它可以体现一个文档中词语在语料库中的重要程度。
    词语由t表示，文档由d表示，语料库由D表示。词频TF(t,d)是词语t在文档d中出现的次数。
    文件频率DF(t,D)是包含词语的文档的个数。
    如果我们只使用词频来衡量重要性，很容易过度强调在文档中经常出现而并没有包含太多与文档有关的信息的词语，比如"a","the"以及"of"。
    如果一个词语经常出现在语料库中，它意味着它并没有携带特定的文档的特殊信息。
  逆向文档频率数值化衡量词语提供多少信息：公式见官方文档
  其中，|D|是语料库中的文档总数。由于采用了对数，如果一个词出现在所有的文件，其IDF值变为0。

   */
  val 特征提取_TF_IDF = 0
  if (0) {
    /*
    在下面的代码段中，我们以一组句子开始。首先使用分解器Tokenizer把句子划分为单个词语。
    对每一个句子（单词包），我们使用HashingTF将句子转换为特征向量，最后使用IDF重新调整特征向量。这种转换通常可以提高使用文本特征的性能。
    然后我们的特征向量可以传递给学习算法。
     */
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // 或者，CountVectorizer也可用于获取术语频率向量

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#word2vec
  Word2vec是一个Estimator，它采用一系列代表文档的词语来训练word2vecmodel。
    该模型将每个词语映射到一个固定大小的向量。word2vecmodel使用文档中每个词语的平均数来将文档转换为向量，
    然后这个向量可以作为预测的特征，来计算文档相似度计算等等。
   */
  val 特征提取_Word2Vec = 0
  if (0) {
    /*
    在下面的代码段中，我们从一组文档开始，每个文档都表示为一系列单词。
    对于每个文档，我们将其转换为特征向量。然后可以将该特征向量传递给学习算法。
     */
    //输入数据：每一行都是句子或者文献中的一个单词。

    import org.apache.spark.ml.feature.Word2Vec
    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.Row

    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // 学习从单词到向量的映射：Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
        .setInputCol("text")
        .setOutputCol("result")
        .setVectorSize(3)
        .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }
  }

  /*
   http://spark.apache.org/docs/2.3.1/ml-features.html#countvectorizer
   计数向量器
     Countvectorizer和Countvectorizermodel旨在通过计数来将一个文档转换为向量。
       当不存在先验字典时，Countvectorizer可作为Estimator来提取词汇，并生成一个Countvectorizermodel。
       该模型产生文档关于词语的稀疏表示，其表示可以传递给其他算法如LDA。
     在拟合过程中，countvectorizer将根据语料库中的词频排序选出vocabsize前几个词。
       一个可选的参数minDF也影响fitting过程中，它指定词汇表中的词语在文档中最少出现的次数。
       另一个可选的二进制切换参数控制输出向量，如果设置为true那么所有非零的计数为1。这对于模拟二进制而非整数计数的离散概率模型特别有用。
    */
  val 特征提取_CountVectorizer = 0
  if (0) {
    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // 从语料库拟合CountVectorizerModel：it a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
        .setInputCol("words")
        .setOutputCol("features")
        .setVocabSize(3)
        .setMinDF(2)
        .fit(df)

    // 或者：使用先验词汇表定义CountVectorizerModel
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
        .setInputCol("words")
        .setOutputCol("features")

    cvModel.transform(df).show(false)
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#featurehasher
  特征散列将一组分类或数字特征投影到指定维度的特征向量中（通常基本上小于原始特征空间的特征向量）。
    这是使用散列技巧将要素映射到特征向量中的索引来完成的。
  该FeatureHasher transformer 在多列上运行。每列可能包含数字或分类功能。列数据类型的行为和处理如下：
    数字列：对于数字要素，列名称的哈希值用于将要素值映射到要素向量中的索引。
      默认情况下，数字要素不被视为分类（即使它们是整数）。要将它们视为分类，请使用categoricalCols参数指定相关列。
    字符串列：对于分类功能，字符串“column_name = value”的哈希值用于映射到矢量索引，指示符值为1.0。
      因此，类别特征是“one-hot”编码（类似于使用OneHotEncoder用 dropLast=false）。
    布尔列：布尔值的处理方式与字符串列相同。也就是说，布尔特征表示为“column_name = true”或“column_name = false”，指示符值为1.0。
  忽略空（缺失）值（在结果特征向量中隐式为零）。

  这里使用的哈希函数也是HashingTF中 使用的MurmurHash 3。
    由于散列值的简单模数用于确定向量索引，因此建议使用2的幂作为numFeatures参数; 否则，特征将不会均匀地映射到矢量索引。
   */
  val 特征提取_FeatureHasher = 0
  if (0) {
    import org.apache.spark.ml.feature.FeatureHasher

    val dataset = spark.createDataFrame(Seq(
      (2.2, true, "1", "foo"),
      (3.3, false, "2", "bar"),
      (4.4, false, "3", "baz"),
      (5.5, false, "4", "foo")
    )).toDF("real", "bool", "stringNum", "string")

    val hasher = new FeatureHasher()
        .setInputCols("real", "bool", "stringNum", "string")
        .setOutputCol("features")

    val featurized = hasher.transform(dataset)
    featurized.show(false)

  }

  val __________________001分割线__________________ = 0

  /*
  https://blog.csdn.net/liulingyuan6/article/details/53397780
  http://spark.apache.org/docs/2.3.1/ml-features.html#tokenizer
   */
  val 特征变换_Tokenizer_分词器 = 0
  if (0) {
    /*
      Tokenization将文本划分为独立个体（通常为单词）。下面的例子展示了如何把句子划分为单词。
        RegexTokenizer基于正则表达式提供更多的划分选项。
          默认情况下，参数“pattern”为划分文本的分隔符。
          或者，用户可以指定参数“gaps”来指明正则“patten”表示“tokens”而不是分隔符，这样来为分词结果找到所有可能匹配的情况。
     */
    import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
    import org.apache.spark.sql.functions._

    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
        .setInputCol("sentence")
        .setOutputCol("words")
        .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val countTokens = udf { (words: Seq[String]) => words.length }

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence", "words")
        .withColumn("tokens", countTokens(col("words"))).show(false)

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence", "words")
        .withColumn("tokens", countTokens(col("words"))).show(false)
  }

  /*
  停用词为在文档中频繁出现，但未承载太多意义的词语，他们不应该被包含在算法输入中。
  StopWordsRemover的输入为一系列字符串（如分词器输出），输出中删除了所有停用词。
    停用词表由stopWords参数提供。一些语言的默认停用词表可以通过StopWordsRemover.loadDefaultStopWords(language)调用。
    布尔参数caseSensitive指明是否区分大小写（默认为否）
   */
  val 特征变换_StopWordsRemover_停用词 = 0
  if (0) {
    import org.apache.spark.ml.feature.StopWordsRemover

    val remover = new StopWordsRemover()
        .setInputCol("raw")
        .setOutputCol("filtered")
    //假设我们有如下DataFrame，有id和raw两列：
    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")
    //通过对raw列调用StopWordsRemover，我们可以得到筛选出的结果列中：“I”, “the”, “had”以及“a”被移除。
    remover.transform(dataSet).show(false)
  }

  /*
  一个n-gram是一个长度为整数n的字序列。NGram可以用来将输入转换为n-gram。
    NGram的输入为一系列字符串（如分词器输出）。
    参数n决定每个n-gram包含的对象个数。
    结果包含一系列n-gram，其中每个n-gram代表一个空格分割的n个连续字符。如果输入少于n个字符串，将没有输出结果。
   */
  val 特征变换_Ngram = 0
  if (0) {
    import org.apache.spark.ml.feature.NGram

    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(false)
  }

  /*
   二值化是根据阀值将连续数值特征转换为0-1特征的过程。
   Binarizer参数有输入、输出以及阀值。特征值大于阀值将映射为1.0，特征值小于等于阀值将映射为0.0。
   */
  val 特征变换_Binarizer_二值化 = 0
  if (0) {
    import org.apache.spark.ml.feature.Binarizer

    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

    val binarizer: Binarizer = new Binarizer()
        .setInputCol("feature")
        .setOutputCol("binarized_feature")
        .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarizedDataFrame.show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#pca
  成分分析是一种统计学方法，它使用正交转换从一系列可能相关的变量中提取线性无关变量集，提取出的变量集中的元素称为主成分。
  使用PCA方法可以对变量集合进行降维。下面的示例将会展示如何将5维特征向量转换为3维主成分向量。
   */
  val 特征变换_PCA_主成分分析 = 0
  if (0) {
    import org.apache.spark.ml.feature.PCA
    import org.apache.spark.ml.linalg.Vectors

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val pca = new PCA()
        .setInputCol("features")
        .setOutputCol("pcaFeatures")
        .setK(3)
        .fit(df)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#polynomialexpansion
  多项式扩展通过产生n维组合将原始特征将特征扩展到多项式空间。下面的示例会介绍如何将你的特征集拓展到3维多项式空间。
   */
  val 特征变换_PolynomialExpansion_多项式扩展 = 0
  if (0) {
    import org.apache.spark.ml.feature.PolynomialExpansion
    import org.apache.spark.ml.linalg.Vectors

    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(3.0, -1.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val polyExpansion = new PolynomialExpansion()
        .setInputCol("features")
        .setOutputCol("polyFeatures")
        .setDegree(3)

    val polyDF = polyExpansion.transform(df)
    polyDF.show(false)
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#discrete-cosine-transform-dct
  离散余弦变换是与傅里叶变换相关的一种变换，它类似于离散傅立叶变换但是只使用实数。
  离散余弦变换相当于一个长度大概是它两倍的离散傅里叶变换，这个离散傅里叶变换是对一个实偶函数进行的
    （因为一个实偶函数的傅里叶变换仍然是一个实偶函数）。
  离散余弦变换，经常被信号处理和图像处理使用，用于对信号和图像（包括静止图像和运动图像）进行有损数据压缩。
   */
  val 特征变换_DCT_离散余弦变换 = 0
  if (0) {
    import org.apache.spark.ml.feature.DCT
    import org.apache.spark.ml.linalg.Vectors

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val dct = new DCT()
        .setInputCol("features")
        .setOutputCol("featuresDCT")
        .setInverse(false)

    val dctDf = dct.transform(df)
    dctDf.select("featuresDCT").show(false)
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#stringindexer
  StringIndexer将字符串标签编码为标签指标。
    指标取值范围为[0,numLabels]，按照标签出现频率排序，所以出现最频繁的标签其指标为0。
    如果输入列为数值型，我们先将之映射到字符串然后再对字符串的值进行指标。
    如果下游的管道节点需要使用字符串－指标标签，则必须将输入和钻还为字符串－指标列名。
   */
  val 特征变换_StringIndexer = 0
  if (0) {
    import org.apache.spark.ml.feature.StringIndexer

    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
        .setInputCol("category")
        .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#indextostring
  与StringIndexer对应，IndexToString将指标标签映射回原始字符串标签。
  一个常用的场景是先通过StringIndexer产生指标标签，然后使用指标标签进行训练，最后再对预测结果使用IndexToString来获取其原始的标签字符串。
   */
  val 特征变换_IndexToString = 0
  if (0) {
    import org.apache.spark.ml.attribute.Attribute
    import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
        .setInputCol("category")
        .setOutputCol("categoryIndex")
        .fit(df)
    val indexed = indexer.transform(df)

    println(s"Transformed string column '${indexer.getInputCol}' " +
        s"to indexed column '${indexer.getOutputCol}'")
    indexed.show()

    val inputColSchema = indexed.schema(indexer.getOutputCol)
    println(s"StringIndexer will store labels in output column metadata: " +
        s"${Attribute.fromStructField(inputColSchema).toString}\n")

    val converter = new IndexToString()
        .setInputCol("categoryIndex")
        .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)

    println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
        s"column '${converter.getOutputCol}' using labels in metadata")
    converted.select("id", "categoryIndex", "originalCategory").show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#onehotencoderestimator
  单热编码将表示为标签索引的分类特征映射到二进制向量，该二进制向量具有至多单个一个值，该值指示所有特征值的集合中存在特定特征值。
    此编码允许期望连续特征（例如Logistic回归）的算法使用分类特征。对于字符串类型输入数据，通常首先使用StringIndexer对分类特征进行编码。
  OneHotEncoderEstimator可以转换多个列，为每个输入列返回一个热编码的输出向量列。通常使用VectorAssembler将这些向量合并为单个特征向量。
  OneHotEncoderEstimator支持handleInvalid参数，以选择在转换数据期间如何处理无效输入。
    可用选项包括'keep'（任何无效输入分配给额外的分类索引）和'error'（抛出错误）。
   */
  val 特征变换_OneHotEncoderEstimator_ = 0
  if (0) {
    import org.apache.spark.ml.feature.OneHotEncoderEstimator

    val df = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    val encoder = new OneHotEncoderEstimator()
        .setInputCols(Array("categoryIndex1", "categoryIndex2"))
        .setOutputCols(Array("categoryVec1", "categoryVec2"))
    val model = encoder.fit(df)

    val encoded = model.transform(df)
    encoded.show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#vectorindexer
   VectorIndexer解决数据集中的类别特征Vector。它可以自动识别哪些特征是类别型的，并且将原始值转换为类别指标。它的处理流程如下：
    1.获得一个向量类型的输入以及maxCategories参数。
    2.基于原始数值识别哪些特征需要被类别化，其中最多maxCategories需要被类别化。
    3.对于每一个类别特征计算0-based类别指标。
    4.对类别特征进行索引然后将原始值转换为指标。
   索引后的类别特征可以帮助决策树等算法处理类别型特征，并得到较好结果。
   在下面的例子中，我们读入一个数据集，然后使用VectorIndexer来决定哪些特征需要被作为非数值类型处理，将非数值型特征转换为他们的索引。
   */
  val 特征变换_VectorIndexer = 0
  if (0) {
    import org.apache.spark.ml.feature.VectorIndexer

    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val indexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexed")
        .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} " +
        s"categorical features: ${categoricalFeatures.mkString(", ")}")

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()
  }

  /*
   Normalizer是一个转换器，它可以将多行向量输入转化为统一的形式。
   参数为p（默认值：2）来指定正则化中使用的p-norm。正则化操作可以使输入数据标准化并提高后期学习算法的效果。
   下面的例子展示如何读入一个libsvm格式的数据，然后将每一行转换为L^2以及L^∞形式。
   */
  val 特征变换_Normalizer_正则化 = 0

  val 特征选择 = 0

}