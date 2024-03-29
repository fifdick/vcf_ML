import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class BinClassificationResult (sparkSession: SparkSession) extends Serializable {

   //case class Prediction(label: Double, propability: Double)

  private var _PRcurve: RDD[(Double,Double)] = sparkSession.sparkContext.parallelize(Seq(Tuple2(0.0,0.0)))
private var _AUCvalue: Double = 0
private var _accuracy : Double = 0
private var _predicitons : DataFrame= sparkSession.emptyDataFrame
private var _baselineAccuracy : Double = 0
 // this is the accuracy meassured on the test set, that was split of the dataset that was usid in th VI analysis (expected to be better than accuracy of the pure testset)
 private var _accuracy_testSetOfVITrain : Double = 0
  private var _Ntop : Int = 0


// can define getters and setters later

// Getter
 def accuracy:Double =_accuracy
  def AUCvalue:Double =_AUCvalue
  def PRcurve:RDD[(Double,Double)] = _PRcurve
  def predictions: DataFrame= _predicitons
  def baselineAccuracy: Double = _baselineAccuracy
 def accuracy_testSetOfVITrain : Double = _accuracy_testSetOfVITrain
def Ntop : Int = _Ntop
 // Setter
 /*
 First, the method name is “age_=“. The underscore is a special character in Scala and in this case,
  allows for a space in the method name which essentially makes the name “age =”.
   The parentheses and contents dictate the value and type that needs to be passed in.
    The “:Unit” code is equivalent to returning void. The remaining code is setting the “_age” variable to “value”.
     These things allow the method to be used in the same way as directly accessing the public property.
  */
 def accuracy_= (value:Double):Unit = _accuracy = value
  def AUCvalue_= (value:Double):Unit =_AUCvalue= value
  def PRcurve_= (value:RDD[(Double,Double)]):Unit = _PRcurve = value
  def predictions_= (value:DataFrame) :Unit= _predicitons = value
  def baselineAccuracy_= (value:Double) : Unit = _baselineAccuracy = value
 def accuracy_testSetOfVITrain_= (value:Double):Unit =_accuracy_testSetOfVITrain = value
def Ntop_=(value:Int):Unit=_Ntop = value

}
