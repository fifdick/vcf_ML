import org.apache.spark.sql.DataFrame

object utils {
 def labelBalanceRatio(dataset: DataFrame): Double = {
      val numNegatives = dataset.filter(dataset("label") === 0).count()
      val datasetSize = dataset.count()
      val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize
      balancingRatio
    }
  case class Label(name: String, value: Long)

  def maxClass(dataset: DataFrame): Int = {
      val numNegatives = dataset.filter(dataset("label") === 0).count()
      val numPositives = dataset.filter(dataset("label") === 1).count()
      val maxClass = List(Label("neg",numNegatives),Label("pos",numPositives)).maxBy(Label => Label.value).name
      if( maxClass == "neg")
        {
          return(0)
        }
      else
        return(1)
    }
}
