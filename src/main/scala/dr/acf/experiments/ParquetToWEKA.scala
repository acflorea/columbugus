package dr.acf.experiments

import dr.acf.recc.{FeatureContext, ReccomenderBackbone}
import dr.acf.spark.SparkOps
import dr.acf.spark.SparkOps._
import org.apache.spark.mllib.linalg.Vector
import org.slf4j.LoggerFactory

/**
  * Created by acflorea on 29/05/16.
  */
object ParquetToWEKA extends SparkOps {

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]) {

    // File System root
    val fsRoot = conf.getString("filesystem.root")
    val includeCategory = conf.getBoolean("preprocess.includeCategory")
    val includeProduct = conf.getBoolean("preprocess.includeProduct")


    val categorySFSize = if (includeCategory) conf.getString("preprocess.categoryScalingFactor").split(",").length - 1 else 0
    val categoryMSize = if (includeCategory) conf.getString("preprocess.categoryMultiplier").split(",").length - 1 else 0
    val productSFSize = if (includeProduct) conf.getString("preprocess.productScalingFactor").split(",").length - 1 else 0
    val productMSize = if (includeProduct) conf.getString("preprocess.productMultiplier").split(",").length - 1 else 0

    for {categorySFIndex <- 0 to categorySFSize
         categoryMIndex <- 0 to categoryMSize
         productSFIndex <- 0 to productSFSize
         productMIndex <- 0 to productMSize
    } {

      val featureContext: FeatureContext =
        ReccomenderBackbone.getFeatureContext(categorySFIndex, categoryMIndex, productSFIndex, productMIndex)

      //val trainingData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_training_data_simple_${ReccomenderBackbone.FileFriendly(featureContext.features.toString)}")
      //val testData = sc.objectFile[LabeledPoint](s"$fsRoot/acf_test_data_simple_${ReccomenderBackbone.FileFriendly(featureContext.features.toString)}")

      val numericalData = sqlContext.read.parquet(s"$fsRoot/acf_numerical_data")

      val selectedData = numericalData.select("assignment_class", "product_id", "component_id", "features")

      val toSave = selectedData.map { row =>
        (row.getDouble(0).toString, row.getInt(1), row.getInt(2), row.getAs[Vector](3).toDense)
      }

      import java.io._

      val fileTitle = "columbugus-model"
      val file = new File(s"$fsRoot/$fileTitle")
      val bw = new BufferedWriter(new FileWriter(file))

      // Header section
      bw.write(s"% $fileTitle\n")
      bw.write(s"%\n")
      bw.write(s"\n")

      // Relation
      bw.write(s"@RELATION $fileTitle\n")
      bw.write(s"\n")

      // Attributes
      selectedData.schema.map { column =>
        bw.write(s"@ATTRIBUTE ${column.name} NUMERIC\n")
      }
      bw.write(s"\n")

      // Data


      // Buh bye
      bw.close()

      logger.debug("Yey")
    }
  }

}
