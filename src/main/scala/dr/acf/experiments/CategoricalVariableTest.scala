package dr.acf.experiments

import dr.acf.common.CategoricalVariable
import dr.acf.spark.SparkOps

/**
  * Created by aflorea on 30.11.2015.
  */
object CategoricalVariableTest extends SparkOps {

  def main(args: Array[String]) {

    val testSeq = Seq("ana", "are", "mere", "ana", "vasile", "mere", "ana")
    val cv = new CategoricalVariable[String](sc.parallelize(testSeq))

    cv.toBinaryList.collect().foreach(println)
  }

}
