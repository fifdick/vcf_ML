import au.csiro.variantspark.api.VSContext
import au.csiro.variantspark.api.ImportanceAnalysis
import au.csiro.variantspark.algo
import au.csiro.variantspark.algo.{DecisionTreeParams, RandomForestParams, WideDecisionTree, WideRandomForest}
import au.csiro.variantspark.data.BoundedOrdinal
import au.csiro.variantspark.input.{FeatureSource, LabelSource}
import au.csiro.variantspark.metrics.Metrics
import au.csiro.variantspark.utils.Projector
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.classification.{GBTClassifier, LinearSVC, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Model, Pipeline}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import java.io
import java.io.{FileOutputStream, ObjectOutputStream}



object VCF_BinaryClassifierPipe {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("spark_test").setMaster("local[15]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    implicit val vsContext : VSContext = VSContext(spark)

    val timeStamp : Long = System.currentTimeMillis / 1000

    val nTreeParams = Array(10,100,1000,10000)
    val mtryFracParams = Array(0.1,0.2,0.25,0.3,0.35,0.4)


    val NtopParams = Array(100,500,1000,2000)


    val featureSource = vsContext.featureSource("/data/content/vcf_classification/data_used/trainSplit.vcf")

    val labelSource = vsContext.labelSource("/data/content/vcf_classification/data_used/labels_train.txt", "label")

    val featureSourceTest=vsContext.featureSource("/data/content/vcf_classification/data_used/testSplit.vcf")
    val labelSourceTest = vsContext.labelSource("/data/content/vcf_classification/data_used/labels_test.txt", "label")


    val TuningObj = new Tuning(spark)

    TuningObj.varImpTuning(vsContext,featureSource,labelSource,nTreeParams,mtryFracParams)
    val filename = "/data/content/vcf_classification/results/tuningVI/" + timeStamp + ".txt"

 // (2) write the instance out to a file
    val oos = new ObjectOutputStream(new FileOutputStream(filename + ".ScalaObj"))
        oos.writeObject(TuningObj)
        oos.close

    TuningObj.TuningResultDf.coalesce(1).write.csv(filename)
    TuningObj.top10s.foreach(println)
// (3) read the object back in
  //  val ois = new ObjectInputStream(new FileInputStream("/tmp/nflx"))
   // val stock = ois.readObject.asInstanceOf[Stock]
    //ois.close


   /*
    val importanceAnalysis = ImportanceAnalysis(featureSource, labelSource, nTrees = TuningObj.bestParam._1, rfParams = RandomForestParams(oob = true, nTryFraction = TuningObj.bestParam._2))

    val NtopResults = NtopParams.map { Ntop =>

      // Create datasets selecting nTop variables
      val data: DataFrame = VCFTransformer.ReverseTransposeVCF(featureSource, labelSource, importanceAnalysis, Ntop, spark)
      val pureTestData: DataFrame = VCFTransformer.ReverseTransposeVCF(featureSourceTest, labelSourceTest, importanceAnalysis, Ntop, spark)


      //** MODEL SELECTION AND FITTING **//
      val splits = data.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))


      println("balance of data:")
      print(utils.labelBalanceRatio(data))
      println("balance of train split")
      print(utils.labelBalanceRatio(trainingData))
      println("balance of test split")
      print(utils.labelBalanceRatio(testData))
      println("balance of pure testData")
      print(utils.labelBalanceRatio(pureTestData))

      //##################BOOSTING############################################

      val Tgbt = new GBTClassifier()
      val paramGrid = new ParamGridBuilder()
        .addGrid(param = Tgbt.maxDepth, values = Array(2, 4, 6, 8, 10, 15))
        .addGrid(param = Tgbt.maxIter, values = Array(10, 15, 20, 50, 100, 1000))
        .addGrid(param = Tgbt.impurity, values = Array("entropy", "gini"))
        .build()

      val pipeline = new Pipeline().setStages(Array(Tgbt))
      // ROC curves are appropriate when the observations are balanced between each class, whereas precision-recall curves are appropriate for imbalanced datasets.

      val GbtEvaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
      val crossVal = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(GbtEvaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(10)
      val CVmodel = crossVal.fit(trainingData)
      print(CVmodel.bestModel.params)
      print(CVmodel.avgMetrics.foreach(println))

      val predictions1 = CVmodel.transform(testData)
      val predictions2 = CVmodel.transform(pureTestData)

      val mostFreqLabel = utils.maxClass(pureTestData)
      val classBalance = utils.labelBalanceRatio(pureTestData)

      val majorityVoteRDD =pureTestData.select("label").rdd.map{ row =>
        val label = row.getDouble(0)
        val prediction = mostFreqLabel
        val propability = classBalance
        Row(label,prediction,propability)
      }
      val schema: StructType=  new StructType()
        .add(StructField("label", DoubleType, true))
        .add(StructField("prediction", IntegerType, true))
        .add(StructField("propability", DoubleType, true))

      val majorityVotePrediction = spark.createDataFrame(majorityVoteRDD,schema)

      //Precision - Recall
      val curve1= Evaluator.evaluateModel_PR(predictions1)
      val curve2= Evaluator.evaluateModel_PR(predictions2)

      // Accuracy
      val accuracy1= Evaluator.evaluateModel_Accuracy(CVmodel, testData, "label")
      val accuracy2 =Evaluator.evaluateModel_Accuracy(CVmodel, pureTestData, "label")
      val accuracyBase= Evaluator.evaluateDF_Accuracy(majorityVotePrediction)

      // AUC ROC
      val AUC1 = GbtEvaluator.evaluate(predictions1)
      println(s"Area under ROC = ${AUC1}")
      val AUC2 = GbtEvaluator.evaluate(predictions2)
      println(s"Area under ROC (pure testData) = ${AUC2}")

      val result = new BinClassificationResult(spark)

      result.accuracy= accuracy2
      result.AUCvalue= AUC2
      result.predictions=predictions2
      result.PRcurve=curve2
      result.baselineAccuracy=accuracyBase

        result



    } // nTop map
    */

  }//main


}//obj
