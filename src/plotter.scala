
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spire.syntax.field
import vegas._
import vegas.render.WindowRenderer._
import vegas.data.External._
import vegas.sparkExt._
import vegas.Field
class plotter(spark: SparkSession) {

  /*def simpleTestPlot(): Unit = {
    val seq = Seq(("a", 16), ("b", 77), ("c", 45), ("d", 101), ("e", 132), ("f", 166), ("g", 51))
    val df = seq.toDF("id", "value")

    df.show()

    val usingSparkdf = Vegas("UsingSpark")
      .withDataFrame(df)
      .encodeX("id", Nom)
      .encodeY("value", Quant)
      .mark(Bar)

    usingSparkdf.show
  }

  def makePlotReadyDF(resultLst: IndexedSeq[BinClassificationResult]): DataFrame = {

    val rdd = spark.sparkContext.parallelize(resultLst).map {
      case res: BinClassificationResult => Row(res.Ntop, res.accuracy, res.baselineAccuracy)
      case None => print("Make sure you have sth in you resultlist if you want to plot- seems the entires are None")
    }
    val schema = new StructType(
      Array(
        StructField("Ntop", IntegerType, true),
        StructField("Accuracy", DoubleType, true),
        StructField("Baseline_Accuracy", DoubleType, true)
      ))
    val df = spark.createDataFrame(rdd,schema)
    df.head()
    df
  }

  def makePlotReadyDF(accuracyFile:String,baselineFile:String,Ntops:Array[Int]) : DataFrame = {
//TODO : check that order of Ntop Array or that order of the lines in the file are exactly as run by Ntop Array...
// (probbaly not since we used coalesce with shuffle = true for savingAsTextFile)
    val accuracyValues = utils.readFile(accuracyFile).map{s => s.toDouble}
    val baselineValues = utils.readFile(accuracyFile).map{s => s.toDouble}

    val accuracies =Ntops.zip(accuracyValues).toMap.toSeq
    val baselines = Ntops.zip(baselineValues).toMap.toSeq

    val merged= accuracies ++ baselines
    val grouped = merged.groupBy{KVp => KVp._1}
    val cleaned = grouped.mapValues{KVp => KVp.map{elem => elem._2}.toList}
    //make sure that order of elements in the list will be as defined in line 52 where merged is defined
    val rdd = spark.sparkContext.parallelize(cleaned.map{z => Row(z._1,z._2(0),z._2(1))})

    val schema = new StructType(
      Array(
        StructField("Ntop", IntegerType, true),
        StructField("Accuracy", DoubleType, true),
        StructField("Baseline_Accuracy", DoubleType, true)
      ))
    val df = park.createDataFrame(rdd,schema)
    df.head()
    df

  }

  def plotAccuracyScatter (df :DataFrame) : Unit = {
    val df_melted = melt(id_vars = Seq("Ntop"), df)
     val usingSparkdf = Vegas("Accuracy vs Ntop",width=800,height=600)
      .withDataFrame(df)
      .encodeX("Ntop", Quant)
      .encodeY("value", Quant)
       .encodeColor(vegas.field="variable",dataType=Nominal, legend=Legend(orient="left", title=""))
      .mark(Line)

    usingSparkdf.show
  }

  def melt(
            id_vars: Seq[String], value_vars: Seq[String],
            var_name: String = "variable", value_name: String = "value",df : DataFrame) : DataFrame = {

        // Create array<struct<variable: str, value: ...>>
        val _vars_and_vals = array((for (c <- value_vars) yield { struct(lit(c).alias(var_name), col(c).alias(value_name)) }): _*)

        // Add to the DataFrame and explode
        val _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

        val cols = id_vars.map(col _) ++ { for (x <- List(var_name, value_name)) yield { col("_vars_and_vals")(x).alias(x) }}

        return _tmp.select(cols: _*)

    }
  */

}

