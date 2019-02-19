import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute._
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Evaluator{

  /**
   * Examine a schema to identify the number of classes in a label column.
   * Returns None if the number of labels is not specified, or if the label column is continuous.
   */
  def getNumClasses(labelSchema: StructField): Option[Int] = {
    Attribute.fromStructField(labelSchema) match {
      case binAttr: BinaryAttribute => Some(2)
      case nomAttr: NominalAttribute => nomAttr.getNumValues
      case _: NumericAttribute | UnresolvedAttribute => None
    }}


def evaluateModel_Accuracy(
      model: Transformer,
      data: DataFrame,
      labelColName: String): Double = {
    val fullPredictions = model.transform(data).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select(labelColName).rdd.map(_.getDouble(0))
    // Print number of classes for reference.
    val numClasses = getNumClasses(fullPredictions.schema(labelColName)) match {
      case Some(n) => n
      case None => throw new RuntimeException(
        "Unknown failure when indexing labels for classification.")
    }
    val accuracy = new MulticlassMetrics(predictions.zip(labels)).accuracy
    println(s"  Accuracy ($numClasses classes): $accuracy")
    accuracy
  }

  def evaluateDF_Accuracy(data:DataFrame) : Double = {
    val predictions = data.select("prediction").rdd.map(_.getDouble(0))
    val labels = data.select("label").rdd.map(_.getDouble(0))
    val numClasses = 2
    val accuracy = new MulticlassMetrics(predictions.zip(labels)).accuracy
    println(s"  Accuracy ($numClasses classes): $accuracy")
    accuracy
  }


  def evaluateModel_PR(predictions: DataFrame,spark : SparkSession) : RDD[(Double,Double)] = {

    val predictions2ProbLabl = predictions.select("label", "probability").rdd.map{case r: Row => (r.getDouble(0), r.getAs[Vector[Double]](1).lift(0).get)}

    val metrics = new BinaryClassificationMetrics(predictions2ProbLabl)
    val precision = metrics.precisionByThreshold()

    //Precision by Threshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }
    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }
    // Precision-Recall Curve
    val PRC = metrics.pr
    return(PRC)

  }
}
