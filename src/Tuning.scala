import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.api.{ImportanceAnalysis, VSContext}
import au.csiro.variantspark.input.{FeatureSource, LabelSource}

object Tuning {

  def varImpTuning(vsContext: VSContext, featureSource: FeatureSource, labelSource: LabelSource, nTreeParams: Array[Int], mtryFracParams: Array[Double]): Tuple2[Int, Double] = {
    val variantSparkModels = (for (nTree <- nTreeParams; mtryFrac <- mtryFracParams) yield {

      val importanceAnalysis = ImportanceAnalysis(featureSource, labelSource, nTrees = nTree, rfParams = RandomForestParams(oob = true, nTryFraction = mtryFrac))(vsContext = vsContext)
      val oobErr: Double = importanceAnalysis.rfModel.oobError
      (oobErr, Tuple2(nTree, mtryFrac))
    }).toMap
    val minErr = variantSparkModels.keys.min
    val bestParams = variantSparkModels.get(minErr)
    bestParams match {
       case Some(i) => {
         println(s"Lowest oobErr was: $minErr with parameters: $i")
         return (i)
       }
       case None => {
         println("Didnt find oobErr (key) in Tuning Map")
         return (null)
       }
      }
  }
}