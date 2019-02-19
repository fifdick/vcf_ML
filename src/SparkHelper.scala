import org.apache.hadoop.fs.{FileSystem, FileUtil}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD

object SparkHelper {

  // This is an implicit class so that saveAsSingleTextFile can be attached to
  // SparkContext and be called like this: sc.saveAsSingleTextFile
  implicit class RDDExtensions(val rdd: RDD[String]) extends AnyVal {

    def saveAsSingleTextFile(path: String): Unit =
      saveAsSingleTextFileInternal(path, None)

    def saveAsSingleTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit =
      saveAsSingleTextFileInternal(path, Some(codec))

    private def saveAsSingleTextFileInternal(
                                              path: String, codec: Option[Class[_ <: CompressionCodec]]
                                            ): Unit = {

      // The interface with hdfs:
      val hdfs = FileSystem.get(rdd.sparkContext.hadoopConfiguration)

      // Classic saveAsTextFile in a temporary folder:
      hdfs.delete(new org.apache.hadoop.fs.Path(s"$path.tmp"), true) // to make sure it's not there already
      codec match {
        case Some(codec) => rdd.saveAsTextFile(s"$path.tmp", codec)
        case None        => rdd.saveAsTextFile(s"$path.tmp")
      }

      // Merge the folder of resulting part-xxxxx into one file:
      hdfs.delete(new org.apache.hadoop.fs.Path(path), true) // to make sure it's not there already
      FileUtil.copyMerge(
        hdfs, new org.apache.hadoop.fs.Path(s"$path.tmp"),
        hdfs, new org.apache.hadoop.fs.Path(path),
        true, rdd.sparkContext.hadoopConfiguration, null
      )

      hdfs.delete(new org.apache.hadoop.fs.Path(s"$path.tmp"), true)
    }
  }
}
