import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.api.{ImportanceAnalysis, VSContext}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.tree.loss.LogLoss
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.commons.io
import spire.macros.Auto.java


object VCF_BinaryClassifierPipe {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_test").setMaster("local[15]").set("park.eventLog.enabled","false").set("spark.logConf","true")
    val spark = SparkSession
      .builder()
      .config(conf)
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("data/content/vcf_classification/tmp/")
    implicit val vsContext: VSContext = VSContext(spark)

    // val timeStamp : Long = System / 1000

    val nTreeParams = Array(10, 100, 1000, 10000)
    val mtryFracParams = Array(0.1, 0.2, 0.25, 0.3, 0.35, 0.4)


    val NtopParams_rdd = spark.sparkContext.parallelize(Array(1,2),numSlices = 2) //,100,10, 500, 1000, 2000))
    val NtopParams = Array(2000)//100,10, 500, 1000, 2000)

    val featureSource = vsContext.featureSource("/data/content/vcf_classification/data_used/trainSplit.vcf")

    val labelSource = vsContext.labelSource("/data/content/vcf_classification/data_used/labels_train.txt", "label")

    val featureSourceTest = vsContext.featureSource("/data/content/vcf_classification/data_used/testSplit.vcf")
    val labelSourceTest = vsContext.labelSource("/data/content/vcf_classification/data_used/labels_test.txt", "label")

    /*   val TuningObj = new Tuning(spark)

       TuningObj.varImpTuning(vsContext,featureSource,labelSource,nTreeParams,mtryFracParams)
       val filename = "/data/content/vcf_classification/results/tuningVI/" + timeStamp + ".csv"

    // (2) write the instance out to a file
       val oos = new ObjectOutputStream(new FileOutputStream(filename + ".ScalaObj"))
           oos.writeObject(TuningObj)
           oos.close
        //TO BE TESTED:
       TuningObj.TuningResultDf.write.format("csv").option("sep",";").option("inferSchema","true").option("header","true").save(filename)
       TuningObj.top10s.foreach(println)
   */
    // (3) read the object back in
    //val ois = new ObjectInputStream(new FileInputStream("/tmp/nflx"))
    //val stock = ois.readObject.asInstanceOf[Stock]
    //ois.close

    val TuningObj = new Tuning(spark)
    TuningObj.bestParam = Tuple2(1, 0.1)

      //TODO: put into function
    val importanceAnalysis = ImportanceAnalysis(featureSource, labelSource, nTrees = TuningObj.bestParam._1, rfParams = RandomForestParams(oob = true, nTryFraction = TuningObj.bestParam._2))
    val vars = importanceAnalysis.importantVariables(2000).map{line => line.productIterator.mkString("\t")}
    val VarFile = "/data/content/vcf_classification/results/topVars"+ TuningObj.bestParam.productIterator.mkString("_") + ".txt"
    //org.apache.commons.io.FileUtils.writeLines(file= VarFile, lines=vars)
    spark.sparkContext.parallelize(vars).coalesce(1,true).saveAsTextFile(VarFile)
   // importanceAnalysis.importantVariables(10).foreach(println)

    val dataTransformed = NtopParams.map{ Ntop =>

      println(s"generating data for Ntop: $Ntop")
        // Create datasets selecting nTop variables
      val dat: DataFrame = VCFTransformer.ReverseTransposeVCF(featureSource, labelSource, importanceAnalysis, Ntop, spark)
      val pureTestDat: DataFrame = VCFTransformer.ReverseTransposeVCF(featureSourceTest, labelSourceTest, importanceAnalysis, Ntop, spark)
      (Ntop -> Tuple2(dat,pureTestDat))
    }





    val NtopResults = dataTransformed.map { m =>

      val Ntop = m._1



      println(s"### $Ntop ###")

      val data=m._2._1
      val pureTestData = m._2._2

      data.show(1)
      data.count()
      //** MODEL SELECTION AND FITTING **//
      val splits = data.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0).toDF(), splits(1).toDF())



      println("balance of data:")
      print(utils.labelBalanceRatio(data))
      println("balance of train split")
      print(utils.labelBalanceRatio(trainingData))
      println("balance of test split")
      print(utils.labelBalanceRatio(testData))
      println("balance of pure testData")
      print(utils.labelBalanceRatio(pureTestData))

      //##################BOOSTING############################################

      val Tgbt = new GBTClassifier().setCheckpointInterval(50).setCacheNodeIds(true)
      println("Losstype:  ")
      print(Tgbt.getLossType)
      val paramGrid = new ParamGridBuilder()
        .addGrid(param = Tgbt.maxDepth, values = Array(2))//, 4,6, 8, 10, 15))
        .addGrid(param = Tgbt.maxIter, values = Array(500))//,50,100,10))
        .addGrid(param = Tgbt.stepSize,values= Array(0.1))//,0.01))//, 50, 100, 1000))
        //.addGrid(param = Tgbt.impurity, values = Array("entropy", "gini"))
        //.addGrid(param= Tgbt.setLossType("LogLoss"))
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
      val modelFilepath= "/data/content/vcf_classification/CV/models/#" + Ntop.toString()
      CVmodel.write.overwrite().save(modelFilepath)
      print(CVmodel.bestModel.params)
      print(CVmodel.avgMetrics.foreach(println))
      //CVmodel.save("/data/content/vcf_classification/models/"+ Ntop + ".obj")

      val predictions1 = CVmodel.transform(testData)
      val predictions2 = CVmodel.transform(pureTestData)

      val mostFreqLabel = utils.maxClass(pureTestData)
      val classBalance = utils.labelBalanceRatio(pureTestData)

      val majorityVoteRDD = pureTestData.select("label").rdd.map { case Row(d:Double) =>
        val label = d
        val prediction = mostFreqLabel
        val propability = classBalance
        Row(label, prediction, propability)
      }
      val schema: StructType = new StructType()
        .add(StructField("label", DoubleType, true))
        .add(StructField("prediction", IntegerType, true))
        .add(StructField("propability", DoubleType, true))

      val majorityVotePrediction = spark.createDataFrame(majorityVoteRDD, schema)
      predictions2.printSchema()
      predictions2.show(1)
      //Precision - Recall
      val curve1 = Evaluator.evaluateModel_PR(predictions1,spark)
      val curve2 = Evaluator.evaluateModel_PR(predictions2,spark)

      // Accuracy
      println("accuracy of VI testset that was included in VI analysis ( expected to be better than real accuracy)")
      val accuracy1 = Evaluator.evaluateModel_Accuracy(CVmodel, testData, "label")
      println("accuracy of pureTestData")
      val accuracy2 = Evaluator.evaluateModel_Accuracy(CVmodel, pureTestData, "label")
      val accuracyBase = Evaluator.evaluateDF_Accuracy(majorityVotePrediction)

      // AUC ROC
      val AUC1 = GbtEvaluator.evaluate(predictions1)
      println(s"Area under ROC = ${AUC1}")
      val AUC2 = GbtEvaluator.evaluate(predictions2)
      println(s"Area under ROC (pure testData) = ${AUC2}")

      val result = new BinClassificationResult(spark) with Serializable

      result.accuracy = accuracy2
      result.AUCvalue = AUC2
      result.predictions = predictions2
      result.PRcurve = curve2
      result.baselineAccuracy = accuracyBase
      result.accuracy_testSetOfVITrain = accuracy1
      result.Ntop=Ntop

      result


    } // nTop map


  utils.writeResults("/data/content/vcf_classification/results/CV/",resultLst = spark.sparkContext.parallelize(NtopResults), sparkObj = spark)

  } //main


}
