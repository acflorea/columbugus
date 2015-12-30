package dr.acf.spark

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

/**
  * Created by aflorea on 30.12.2015.
  */
class CategoricalToVector[T](override val uid: String, distinctValues: Seq[T])
  extends UnaryTransformer[T, Seq[Double], CategoricalToVector[T]] {

  def this(distinctValues: Seq[T]) = this(Identifiable.randomUID("CatToVec"), distinctValues)

  override protected def createTransformFunc: (T) => Seq[Double] = {
    v =>
      val pos = distinctValues.indexOf(v)
      val length = distinctValues.length
      List.tabulate(length)(i => if (i == pos) 1.0 else 0.0)
  }

  override protected def outputDataType: DataType = new ArrayType(DoubleType, true)
}
