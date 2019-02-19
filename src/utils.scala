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





  def writeResults(filepath: String, resultLst: IndexedSeq[BinClassificationResult], sparkObj: SparkSession): Unit = {
    resultLst.zipWithIndex.map {
      //Returns the precision-recall curve, which is an RDD of (recall, precision), NOT (precision, recall), with (0.0, p) prepended to it, where p is the precision associated with the lowest recall on the curve
      case res: (BinClassificationResult,Int) => println(res._1.PRcurve.count())
        res._1.PRcurve.coalesce(1,true).saveAsTextFile(filepath + "_RP_" + res._2 + ".txt")
        //spark.createDataFrame(res.PRcurve).toDF("Recall","Precision")
    }


    val accuracies = resultLst.map(r => r.accuracy).toArray
    sparkObj.sparkContext.parallelize(accuracies).coalesce(1,true).saveAsTextFile(filepath + "_accuracies.txt")
    //sparkObj.sparkContext.parallelize(accuracies).map(a => a.toString()).saveAsSingleTextFile(filepath + "_accuraciesAsStrings.txt")

    val base= resultLst.map(res => res.baselineAccuracy).toArray
    sparkObj.sparkContext.parallelize(base).coalesce(1,true).saveAsTextFile(filepath + "_bases.txt")

    val aucs = resultLst.map(res => res.AUCvalue).toArray
    sparkObj.sparkContext.parallelize(aucs).coalesce(1,true).saveAsTextFile(filepath + "_AUCs.txt")

  }

  def readFile(filename: String): Array[String] = {
    val bufferedSource = io.Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close
    lines
}




}
