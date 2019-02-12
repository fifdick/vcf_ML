import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.api.{ImportanceAnalysis, VSContext}
import au.csiro.variantspark.input.{FeatureSource, LabelSource}
import ca.innovativemedicine.vcf.Type
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

@SerialVersionUID(100L)
class Tuning(sparkSession: SparkSession) extends Serializable {

 val schema = new StructType()
  schema.add(StructField("oobErr", DoubleType))
  schema.add(StructField("nTree", IntegerType))
  schema.add(StructField("mTry", DoubleType))



  // vars
  private var _bestParam: Tuple2[Int, Double] = (0, 0)
  private var _minOOB: Double = 1
  private var _TuningResultDf: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],schema)
  private var _top10s: Array[String] = Array("")




  // Getter
  def bestParam: Tuple2[Int, Double] = _bestParam
  def minOOB: Double = _minOOB
  def TuningResultDf: DataFrame = _TuningResultDf
  def top10s: Array[String] = _top10s

  // Setter
  def bestParam_=(value: Tuple2[Int, Double]): Unit = _bestParam = value
  def minOOB_=(value: Double): Unit = _minOOB = value
  def TuningResultDf_=(value: DataFrame): Unit = _TuningResultDf = value
  def top10s_=(value: Array[String]): Unit = _top10s = value

  //methods

  def varImpTuning(vsContext: VSContext, featureSource: FeatureSource, labelSource: LabelSource, nTreeParams: Array[Int], mtryFracParams: Array[Double], spark: SparkSession): Unit = {

    val variantSparkModels = (for (nTree <- nTreeParams; mtryFrac <- mtryFracParams) yield {

      val importanceAnalysis = ImportanceAnalysis(featureSource, labelSource, nTrees = nTree, rfParams = RandomForestParams(oob = true, nTryFraction = mtryFrac))(vsContext = vsContext)
      val oobErr: Double = importanceAnalysis.rfModel.oobError
      print(s"oobErr: $oobErr - mTryFrac: $mtryFrac - nTree: $nTree")
      val top10 = importanceAnalysis.importantVariables(10).map { x => x._1 }.mkString(";")
      (oobErr, Tuple2(Tuple2(nTree, mtryFrac), top10))
    }).toMap
    _minOOB = variantSparkModels.keys.min
    val rdd = sparkSession.sparkContext.parallelize(variantSparkModels.map { x => Tuple2(x._1, x._2._1) }.toSeq)
      .map { x => Row(x._1, x._2._1, x._2._1) }
    _TuningResultDf = sparkSession.createDataFrame(rdd, this.schema)
    _top10s = variantSparkModels.map { x => x._2._2 }.toArray

    val ParamsOpt = variantSparkModels.get(_minOOB)

    ParamsOpt match {
      case Some(i) => {
        println(s"Lowest oobErr was: ${_minOOB} with parameters: ${i._1}")
        _bestParam = i._1

      }
      case None => {
        println("Didnt find oobErr (key) in Tuning Map")
      }
    }
  }
}