package dr.acf.common

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by aflorea on 30.11.2015.
  */
class CategoricalVariable[T](values: RDD[T]) extends Variable {

  lazy val distinctValues = values.distinct().collect()

  /**
    * Converts a categorical variable to a list of binary values
    * The list has the same length as the number of possible values
    * (1 on the position corresponding to the selected value, 0 otherwise)
    */
  def toBinaryList(implicit ct: ClassTag[Array[T]], sc: SparkContext) = {
    val bDistinctValues = sc.broadcast[Array[T]](distinctValues)

    values.map { v =>
      val lDistinctValues = bDistinctValues.value
      val pos = lDistinctValues.indexOf(v)
      val length = lDistinctValues.length
      List.tabulate(length)(i => if (i == pos) 1 else 0)
    }
  }

}
