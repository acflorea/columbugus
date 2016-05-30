package dr.acf.experiments

import dr.acf.recc.{FeatureContext, ReccomenderBackbone}
import dr.acf.spark.SparkOps
import dr.acf.spark.SparkOps._
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.DataType
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

      val selectedData = numericalData.select("features", "assignment_class", "product_id", "component_id")

      val sample = selectedData.take(1).head
      val indexes = (0 until sample.size).toSeq.scan(0) { (x, y) =>
        x + (sample.get(x) match {
          case vector: SparseVector => vector.size
          case _ => 1
        })
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
        bw.write(s"@ATTRIBUTE ${column.name} ${arffType(column.dataType)}\n")
      }
      bw.write(s"\n")

      // Data


      // Buh bye
      bw.close()

      logger.debug("Yey")
    }
  }


  def arffType(dataType: DataType): String = {
    dataType.typeName match {
      case "double" => "NUMERIC"
      case "integer" => "NUMERIC"
      case "vector" => "NUMERIC"
      case _ => ""
    }
  }

}
