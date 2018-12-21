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
  import spark.implicits._

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
  val 特征变换_OneHotEncoderEstimator = 0
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
  http://spark.apache.org/docs/2.3.1/ml-features.html#interaction
  Interaction是一个Transformer，它接收向量或双值列，并生成一个向量列，其中包含每个输入列的一个值的所有组合的乘积。
  例如，如果您有2个矢量类型列，每个列都有3个维度作为输入列，那么您将获得9维向量作为输出列。
   */
  val 特征变换_Interaction = 0
  if (0) {
    import org.apache.spark.ml.feature.Interaction
    import org.apache.spark.ml.feature.VectorAssembler

    val df = spark.createDataFrame(Seq(
      (1, 1, 2, 3, 8, 4, 5),
      (2, 4, 3, 8, 7, 9, 8),
      (3, 6, 1, 9, 2, 3, 6),
      (4, 10, 8, 6, 9, 4, 5),
      (5, 9, 2, 7, 10, 7, 3),
      (6, 1, 1, 4, 2, 8, 4)
    )).toDF("id1", "id2", "id3", "id4", "id5", "id6", "id7")

    val assembler1 = new VectorAssembler().
        setInputCols(Array("id2", "id3", "id4")).
        setOutputCol("vec1")

    val assembled1 = assembler1.transform(df)

    val assembler2 = new VectorAssembler().
        setInputCols(Array("id5", "id6", "id7")).
        setOutputCol("vec2")

    val assembled2 = assembler2.transform(assembled1).select("id1", "vec1", "vec2")

    val interaction = new Interaction()
        .setInputCols(Array("id1", "vec1", "vec2"))
        .setOutputCol("interactedCol")

    val interacted = interaction.transform(assembled2)

    interacted.show(truncate = false)
  }
  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#normalizer
   Normalizer是一个转换器，它可以将多行向量输入转化为统一的形式。
   参数为p（默认值：2）来指定正则化中使用的p-norm。正则化操作可以使输入数据标准化并提高后期学习算法的效果。
   下面的例子展示如何读入一个libsvm格式的数据，然后将每一行转换为L^1以及L^∞形式。
   */
  val 特征变换_Normalizer_正则化 = 0
  if (0) {
    import org.apache.spark.ml.feature.Normalizer
    import org.apache.spark.ml.linalg.Vectors

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    // Normalize each Vector using $L^1$ norm.
    val normalizer = new Normalizer()
        .setInputCol("features")
        .setOutputCol("normFeatures")
        .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)
    println("Normalized using L^1 norm")
    l1NormData.show()

    // Normalize each Vector using $L^\infty$ norm.
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    println("Normalized using L^inf norm")
    lInfNormData.show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#standardscaler
  StandardScaler处理Vector数据，标准化每个特征使得其有统一的标准差以及（或者）均值为零。它需要如下参数：
    1. withStd：默认值为真，使用统一标准差方式。
    2. withMean：默认为False。在缩放之前使用均值将数据居中。它将构建密集输出，因此在应用稀疏输入时要小心。
  StandardScaler是一个Estimator，它可以fit数据集产生一个StandardScalerModel，用来计算汇总统计。
    然后产生的模可以用来转换向量至统一的标准差以及（或者）零均值特征。注意如果特征的标准差为零，则该特征在向量中返回的默认值为0.0。
 下面的示例展示如果读入一个libsvm形式的数据以及返回有统一标准差的标准化特征。
   */
  val 特征变换_StandardScaler = 0
  if (0) {
    import org.apache.spark.ml.feature.StandardScaler

    val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")
        .setWithStd(true)
        .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#minmaxscaler
   MinMaxScaler通过重新调节大小将Vector形式的列转换到指定的范围内，通常为[0,1]，它的参数有：
    1. min：默认为0.0，为转换后所有特征的下边界。
    2. max：默认为1.0，为转换后所有特征的下边界。
   MinMaxScaler计算数据集的汇总统计量，并产生一个MinMaxScalerModel。该模型可以将独立的特征的值转换到指定的范围内。
   对于特征E来说，调整后的特征值如下：见文档
   注意因为零值转换后可能变为非零值，所以即便为稀疏输入，输出也可能为稠密向量。
   下面的示例展示如果读入一个libsvm形式的数据以及调整其特征值到[0,1]之间。
   */
  val 特征变换_MinMaxScaler = 0
  if (0) {
    import org.apache.spark.ml.feature.MinMaxScaler
    import org.apache.spark.ml.linalg.Vectors

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "features")

    val scaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(dataFrame)
    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
    scaledData.select("features", "scaledFeatures").show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#maxabsscaler
  MaxAbsScaler使用每个特征的最大值的绝对值将输入向量的特征值转换到[-1,1]之间。因为它不会转移／集中数据，所以不会破坏数据的稀疏性。
  MaxAbsScaler计算数据集的摘要统计信息并生成MaxAbsScalerModel。然后，模型可以将每个特征单独转换为范围[-1,1]。
  下面的示例展示如果读入一个libsvm形式的数据以及调整其特征值到[-1,1]之间。
   */
  val 特征变换_MaxAbsScaler = 0
  if (0) {
    import org.apache.spark.ml.feature.MaxAbsScaler
    import org.apache.spark.ml.linalg.Vectors

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -8.0)),
      (1, Vectors.dense(2.0, 1.0, -4.0)),
      (2, Vectors.dense(4.0, 10.0, 8.0))
    )).toDF("id", "features")

    val scaler = new MaxAbsScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MaxAbsScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [-1, 1]
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.select("features", "scaledFeatures").show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#bucketizer
   Bucketizer将一列连续的特征转换为特征区间，区间由用户指定。参数如下：
    1. splits：分裂数为n+1时，将产生n个区间。除了最后一个区间外，每个区间范围［x,y］由分裂的x，y决定。
       分裂必须是严格递增的。在分裂指定外的值将被归为错误。
       两个分裂的例子为Array(Double.NegativeInfinity,0.0, 1.0, Double.PositiveInfinity)以及Array(0.0, 1.0, 2.0)。
       注意，当不确定分裂的上下边界时，应当添加Double.NegativeInfinity和Double.PositiveInfinity以免越界。
    下面将展示Bucketizer的使用方法。
   */
  val 特征变换_Bucketizer = 0
  if (0) {
    import org.apache.spark.ml.feature.Bucketizer

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
        .setInputCol("features")
        .setOutputCol("bucketedFeatures")
        .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length - 1} buckets")
    bucketedData.show()

    val splitsArray = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.0, 0.3, Double.PositiveInfinity))

    val data2 = Array(
      (-999.9, -999.9),
      (-0.5, -0.2),
      (-0.3, -0.1),
      (0.0, 0.0),
      (0.2, 0.4),
      (999.9, 999.9))
    val dataFrame2 = spark.createDataFrame(data2).toDF("features1", "features2")

    val bucketizer2 = new Bucketizer()
        .setInputCols(Array("features1", "features2"))
        .setOutputCols(Array("bucketedFeatures1", "bucketedFeatures2"))
        .setSplitsArray(splitsArray)

    // Transform original data into its bucket index.
    val bucketedData2 = bucketizer2.transform(dataFrame2)

    println(s"Bucketizer output with [" +
        s"${bucketizer2.getSplitsArray(0).length - 1}, " +
        s"${bucketizer2.getSplitsArray(1).length - 1}] buckets for each input column")
    bucketedData2.show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#elementwiseproduct
    ElementwiseProduct按提供的“weight”向量，返回与输入向量元素级别的乘积。
    即是说，按提供的权重分别对输入数据进行缩放，得到输入向量v以及权重向量w的Hadamard积。
   */
  val 特征变换_ElementwiseProduct = 0
  if (0) {
    import org.apache.spark.ml.feature.ElementwiseProduct
    import org.apache.spark.ml.linalg.Vectors

    // Create some vector data; also works for sparse vectors
    val dataFrame = spark.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct()
        .setScalingVec(transformingVector)
        .setInputCol("vector")
        .setOutputCol("transformedVector")

    // Batch transform the vectors to create new column:
    transformer.transform(dataFrame).show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#sqltransformer
  SQLTransformer工具用来转换由SQL定义的陈述。
  目前仅支持SQL语法如"SELECT ...FROM __THIS__ ..."，其中"__THIS__"代表输入数据的基础表。
  选择语句指定输出中展示的字段、元素和表达式，支持Spark SQL中的所有选择语句。
  用户可以基于选择结果使用Spark SQL建立方程或者用户自定义函数。SQLTransformer支持语法示例如下：
    1. SELECTa, a + b AS a_b FROM __THIS__
    2. SELECTa, SQRT(b) AS b_sqrt FROM __THIS__ where a > 5
    3. SELECTa, b, SUM(c) AS c_sum FROM __THIS__ GROUP BY a, b
   */
  val 特征变换_SQLTransformer = 0
  if (0) {
    import org.apache.spark.ml.feature.SQLTransformer
    val df = spark.createDataFrame(Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")
    val sqlTrans = new SQLTransformer().setStatement("SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")
    sqlTrans.transform(df).show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#vectorassembler
  VectorAssembler是一个转换器，它将给定的若干列合并为一列向量。
  它可以将原始特征和一系列通过其他转换器得到的特征合并为单一的特征向量，来训练如逻辑回归和决策树等机器学习算法。
  VectorAssembler可接受的输入列类型：数值型、布尔型、向量型。输入列的值将按指定顺序依次添加到一个新向量中。
   */
  val 特征变换_VectorAssembler = 0
  if (0) {
    import org.apache.spark.ml.feature.VectorAssembler
    import org.apache.spark.ml.linalg.Vectors

    val dataset = spark.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
        .setInputCols(Array("hour", "mobile", "userFeatures"))
        .setOutputCol("features")

    val output = assembler.transform(dataset)
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#vectorsizehint
  有时可以明确指定VectorType列的向量大小。
    例如，VectorAssembler使用其输入列中的大小信息来为其输出列生成大小信息和元数据。
    虽然在某些情况下可以通过检查列的内容来获得此信息，但是在流数据帧中，在流启动之前内容不可用。
    VectorSizeHint允许用户显式指定列的向量大小，以便VectorAssembler或可能需要知道向量大小的其他变换器可以将该列用作输入。
  要使用VectorSizeHint，用户必须设置inputCol和size参数。
    将此转换器应用于数据框会生成一个新的数据框，其中包含inputCol的更新元数据，用于指定矢量大小。
    结果数据帧的下游操作可以使用meatadata获得此大小。
  VectorSizeHint还可以使用一个可选的handleInvalid参数，该参数在向量列包含空值或大小错误的向量时控制其行为。
    默认情况下，handleInvalid设置为“error”，表示应抛出异常。
    此参数也可以设置为“skip”，表示应该从结果数据帧中过滤掉包含无效值的行，
    或“optimistic”，表示不应检查列是否存在无效值，并且应保留所有行。
    请注意，使用“optimistic”会导致生成的数据帧处于不一致状态，
    me：应用VectorSizeHint列的元数据与该列的内容不匹配。用户应注意避免这种不一致的状态。
   */
  val 特征变换_VectorSizeHint = 0
  if (0) {
    import org.apache.spark.ml.feature.{VectorAssembler, VectorSizeHint}
    import org.apache.spark.ml.linalg.Vectors

    val dataset = spark.createDataFrame(
      Seq(
        (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
        (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val sizeHint = new VectorSizeHint()
        .setInputCol("userFeatures")
        .setHandleInvalid("skip")
        .setSize(3)

    val datasetWithSize = sizeHint.transform(dataset)
    println("Rows where 'userFeatures' is not the right size are filtered out")
    datasetWithSize.show(false)

    val assembler = new VectorAssembler()
        .setInputCols(Array("hour", "mobile", "userFeatures"))
        .setOutputCol("features")

    // This dataframe can be used by downstream transformers as before
    val output = assembler.transform(datasetWithSize)
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#quantilediscretizer
  QuantileDiscretizer采用具有连续特征的列，并输出具有分箱分类特征的列。
    bin的数量由numBuckets参数设置。所使用的桶的数量可能小于该值，例如，如果输入的不同值太少而不能创建足够的不同分位数。
    NaN值：在QuantileDiscretizer拟合期间，NaN值将从柱中移除。这将产生用于进行预测的Bucketizer模型。
    在转换过程中，Bucketizer会在数据集中找到NaN值时引发错误，但用户也可以选择通过设置handleInvalid来保留或删除数据集中的NaN值。
    如果用户选择保留NaN值，它们将被专门处理并放入自己的桶中，例如，如果使用4个桶，那么非NaN数据将被放入桶[0-3]，但是NaN将是计入一个特殊的桶[4]。
  算法：使用近似算法选择bin范围（有关详细说明，请参阅aboutQuantile的文档）。
    可以使用relativeError参数控制近似的精度。设置为零时，将计算精确分位数（注意：计算精确分位数是一项昂贵的操作）。
    下边界和上边界将是-Infinity和+ Infinity，覆盖所有实际值。

   */
  val 特征变换_QuantileDiscretizer = 0
  if (0) {
    import org.apache.spark.ml.feature.QuantileDiscretizer

    val data = Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    val df = spark.createDataFrame(data).toDF("id", "hour")

    val discretizer = new QuantileDiscretizer()
        .setInputCol("hour")
        .setOutputCol("result")
        .setNumBuckets(3)

    val result = discretizer.fit(df).transform(df)
    result.show(false)
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#imputer
  Imputer变换器使用缺失值所在的列的平均值或中值来完成数据集中的缺失值。
    输入列应为DoubleType或FloatType。目前，Imputer不支持分类功能，并且可能为包含分类功能的列创建不正确的值。
    Imputer可以通过.setMissingValue（custom_value）将除“NaN”以外的自定义值包括在内。例如，.setMissingValue（0）将计算所有出现的（0）。
   */
  val 特征变换_Imputer = 0
  if (0) {
    import org.apache.spark.ml.feature.Imputer

    val df = spark.createDataFrame(Seq(
      (1.0, Double.NaN),
      (2.0, Double.NaN),
      (Double.NaN, 3.0),
      (4.0, 4.0),
      (5.0, 5.0)
    )).toDF("a", "b")

    val imputer = new Imputer()
        .setInputCols(Array("a", "b"))
        .setOutputCols(Array("out_a", "out_b"))

    val model = imputer.fit(df)
    model.transform(df).show()
  }

  val ____________________002分割线____________________ = 0

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#feature-selectors
  https://blog.csdn.net/liulingyuan6/article/details/53413728
   */
  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#vectorslicer
  VectorSlicer是一个变换器，输入特征向量，输出原始特征向量子集。它对于从向量列中提取要素非常有用。
  VectorSlicer接收带有特定索引的向量列，通过对这些索引的值进行筛选得到新的向量集。可接受如下两种索引:
    整数索引，表示向量中的索引，setIndices()。
    字符串索引，表示向量中的要素名称，setNames()。这要求向量列具有AttributeGroup，因为实现与Attribute的name字段匹配。
  指定整数或者字符串类型都是可以的。另外，同时使用整数索引和字符串名字也是可以的。
  不允许使用重复的特征，所以所选的索引或者名字必须是没有独一的。注意如果使用名字特征，当遇到空值的时候将会报错。
  输出将会首先按照所选的数字索引排序（按输入顺序），其次按名字排序（按输入顺序）。
   */
  val 特征选择_VectorSlicer = 0
  if (0) {
    import java.util.Arrays

    import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
    import org.apache.spark.ml.feature.VectorSlicer
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.StructType

    val data = Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataset = spark.createDataFrame(data, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)
    output.show(false)
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#rformula
  Formula通过R模型公式来选择列。支持R操作中的部分操作，包括‘~’, ‘.’, ‘:’, ‘+’以及‘-‘，基本操作如下
   */
  val 特征选择_RFormula = 0
  if (0) {
    import org.apache.spark.ml.feature.RFormula

    val dataset = spark.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")

    val formula = new RFormula()
        .setFormula("clicked ~ country + hour")
        .setFeaturesCol("features")
        .setLabelCol("label")

    val output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#chisqselector
    ChiSqSelector代表卡方特征选择。它适用于带有类别特征的标签数据。
    ChiSqSelector根据类别的独立卡方2检验来对特征排序，然后选取类别标签主要依赖的特征。它类似于选取最有预测能力的特征。
   */
  val 特征选择_ChiSqSelector = 0
  if (0) {
    import org.apache.spark.ml.feature.ChiSqSelector
    import org.apache.spark.ml.linalg.Vectors

    val data = Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    )

    val df = spark.createDataset(data).toDF("id", "features", "clicked")

    val selector = new ChiSqSelector()
        .setNumTopFeatures(1)
        .setFeaturesCol("features")
        .setLabelCol("clicked")
        .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)

    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result.show()
  }

  val ___________________003分割线_____________________ = 0

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#locality-sensitive-hashing
  局部敏感散列（LSH）是一类重要的散列技术，常用于聚类，近似最近邻搜索和大数据集的异常检测。
  LSH的一般思想是使用一系列函数（“LSH系列”）将数据点哈希到桶中，使得彼此接近的数据点在相同的桶中具有高概率，
    而数据点是远离彼此很可能在不同的桶中。 LSH家族的正式定义如下。
  在度量空间（M，d）中，其中M是集合，d是M上的距离函数，LSH族是满足以下属性的函数族h： 见文档

  我们描述了LSH可用于的主要操作类型。拟合的LSH模型具有用于这些操作中的每一个的方法。
   */
  val 局部敏感哈希_Locality_Sensitive_Hashing = 0

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#bucketed-random-projection-for-euclidean-distance   */
  val LSH_Algorithms_01 = 0
  if (0) {
    import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.functions.col

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "features")

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH()
        .setBucketLength(2.0)
        .setNumHashTables(3)
        .setInputCol("features")
        .setOutputCol("hashes")

    val model = brp.fit(dfA)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate
    // similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxSimilarityJoin(transformedA, transformedB, 1.5)`
    println("Approximately joining dfA and dfB on Euclidean distance smaller than 1.5:")
    model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
        .select(col("datasetA.id").alias("idA"),
          col("datasetB.id").alias("idB"),
          col("EuclideanDistance")).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
    // neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxNearestNeighbors(transformedA, key, 2)`
    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()
  }

  /*
  http://spark.apache.org/docs/2.3.1/ml-features.html#minhash-for-jaccard-distance
   */
  val LSH_Algorithms_02 = 0
  if (0) {
    import org.apache.spark.ml.feature.MinHashLSH
    import org.apache.spark.ml.linalg.Vectors
    import org.apache.spark.sql.functions.col

    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
      (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
      (4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
      (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
    )).toDF("id", "features")

    val key = Vectors.sparse(6, Seq((1, 1.0), (3, 1.0)))

    val mh = new MinHashLSH()
        .setNumHashTables(5)
        .setInputCol("features")
        .setOutputCol("hashes")

    val model = mh.fit(dfA)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate
    // similarity join.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxSimilarityJoin(transformedA, transformedB, 0.6)`
    println("Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:")
    model.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
        .select(col("datasetA.id").alias("idA"),
          col("datasetB.id").alias("idB"),
          col("JaccardDistance")).show()

    // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
    // neighbor search.
    // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
    // `model.approxNearestNeighbors(transformedA, key, 2)`
    // It may return less than 2 rows when not enough approximate near-neighbor candidates are
    // found.
    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()
  }

}