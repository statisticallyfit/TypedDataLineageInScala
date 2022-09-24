package TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer


import org.apache.spark.sql.SparkSession
import shapeless.HNil

/**
 * Need way to produce annotated dataframe instances - create type class DataSource.
 * @tparam D
 * @tparam P
 */
// TODO - meaning of params D, P?
// TODO -- mean of D == dataset parameters?
// TODO -- meag of P == custom source parameters? (https://hyp.is/khcmoDTzEe2iyyMb-Wl4_Q/itnext
//  .io/making-the-spark-dataframe-composition-type-safe-r-7b6fed524ec2)

trait DataSource[D, P] {
	def read(parameters: P)(implicit evSpark: SparkSession): AnnotatedDataFrame[D, HNil]
}

object DataSource {

	def apply[D]: Helper[D] = new Helper[D]

	// Helper is used to improve type inference and make API read more cleanly.
	final class Helper[D] {
		def read[P](parameters: P)(implicit evDataSrc: DataSource[D, P],
							  evSpark: SparkSession): AnnotatedDataFrame[D, HNil] ={

			evDataSrc.read(parameters = parameters)
		}

		def read(implicit evDataSrc: DataSource[D, Unit], s: SparkSession): AnnotatedDataFrame[D, HNil] =
			evDataSrc.read(parameters = ())
	}
}