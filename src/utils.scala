import org.apache.spark.sql.{DataFrame, SparkSession}

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
    val maxClass = List(Label("neg", numNegatives), Label("pos", numPositives)).maxBy(Label => Label.value).name
    if (maxClass == "neg") {
      return (0)
    }
    else
      return (1)
  }

  /*def appendToFile(fileName:String, textData:String) =
  using (new FileWriter(fileName, true)){
    fileWriter => using (new PrintWriter(fileWriter)) {
      printWriter => printWriter.println(textData)
    }
  }
*/
  def writeResults(filepath: String, resultLst: IndexedSeq[BinClassificationResult], spark: SparkSession): Unit = {


    resultLst.map {
      //Returns the precision-recall curve, which is an RDD of (recall, precision), NOT (precision, recall), with (0.0, p) prepended to it, where p is the precision associated with the lowest recall on the curve
      case res: BinClassificationResult => res.PRcurve.zipWithIndex().map { x => x._1.coalesce(1, false).saveAsTextFile(filepath + "_RP_" + x._2 + ".txt")
        //spark.createDataFrame(res.PRcurve).toDF("Recall","Precision")
      }
    }


    val accuracies = resultLst.map(r => r.accuracy)
    spark.sparkContext.parallelize(accuracies.toArray).saveAsTextFile(filepath + "_accuracies.txt")
  }
}