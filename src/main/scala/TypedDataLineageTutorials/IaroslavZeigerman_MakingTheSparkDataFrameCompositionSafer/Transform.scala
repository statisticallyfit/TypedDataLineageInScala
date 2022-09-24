package TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer

import TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer.TypeJoinList.JoinList
import org.apache.spark.sql.SparkSession
import shapeless.HNil

/**
 * Transform type class
 */
trait Transform[I, IJ <: JoinList, O, P] {

	def transform(input: AnnotatedDataFrame[I, IJ], parameters: P)
			   (implicit evSpark: SparkSession): AnnotatedDataFrame[O, HNil]
}