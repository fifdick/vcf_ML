import au.csiro.variantspark.api.{ImportanceAnalysis, VSContext}
import au.csiro.variantspark.input.{FeatureSource, LabelSource}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object VCFTransformer {

  def ReverseTransposeVCF(featureSource: FeatureSource, labelSource: LabelSource, importanceAnalysis: ImportanceAnalysis, Ntop: Int, spark: SparkSession ) : DataFrame= {
    //val t1 = System.nanoTime
    val topVars = importanceAnalysis.importantVariables(nTopLimit = Ntop)
    topVars.foreach(println)
    val vars  = topVars.map(x => x._1)
    //filter featureSource to only top vars
    val subsettedFeatures = featureSource.features().filter(x => vars.toArray contains (x.label))
    //println("Subsetting features by top most important, runtime:")
    //println((System.nanoTime - t1) / 1e9d)


    println("imp vars")
    vars.foreach(println)

    println("subsetted Features")
    subsettedFeatures.foreach { x =>
      println(x.label)
      println(x.toVector.values.toString)
    }

    val t2 = System.nanoTime
    val subsettedFeatureMap = subsettedFeatures.map { f =>
      f
      val feature_values = f.toVector.values.toArray
      val feature_name = f.label
      feature_name -> feature_values //Tuple2[String,List[Double ]](feature_name,features_values)
    }.collectAsMap() //HERE I COLLECT AND LOOSE RDD TYPE
    println("Making map of feature names and values, runtime:")
    println((System.nanoTime - t2) / 1e9d)

    val t3 = System.nanoTime
    val transposedMap = subsettedFeatureMap.map { m =>
      m._2.map(m._1 -> _)
    }.toList.transpose.map(_.toMap)
    println("Transposing of map, runtime")
    println((System.nanoTime - t3) / 1e9d)
    println("transposed featureMap:")
    print(transposedMap)
    println("")

    val labels = labelSource.getLabels(featureSource.sampleNames)
    val featuresAndlablelsTransposed = transposedMap.zip(labels).map { case (a, b) => (a, b) }
    println("transposed featureMap:")
    print(featuresAndlablelsTransposed)

    println("")


    //val sc = vsContext.sc
    //val data : RDD[LabeledPoint]= sc.parallelize(featuresAndlablelsTransposed.map { m =>
    //  org.apache.spark.mllib.regression.LabeledPoint(m._2, org.apache.spark.mllib.linalg.Vectors.dense(m._1.values.toArray))
    // })
    val data : DataFrame = spark.createDataFrame(featuresAndlablelsTransposed.map { m =>
      org.apache.spark.ml.feature.LabeledPoint(m._2, org.apache.spark.ml.linalg.Vectors.dense(m._1.values.toArray))
    })

    data.printSchema()

    return data
  }

}
