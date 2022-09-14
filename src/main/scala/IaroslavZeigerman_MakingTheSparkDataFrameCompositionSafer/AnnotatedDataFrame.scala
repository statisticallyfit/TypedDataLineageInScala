package IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer


import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast
import shapeless.ops.hlist.Prepend
import shapeless.{::, HList, HNil}


/**
 * Tutorial source = https://itnext.io/making-the-spark-dataframe-composition-type-safe-r-7b6fed524ec2
 */

object flow {
	type JoinList = HList

	// Each dataframe instance needs a "type tag" to indicate a source from which the instance originates.
	case class AnnotatedDataFrame[D](toDF: DataFrame) extends Serializable


	// Need way to produce annotated dataframe instances - create type class DataSource.
	trait DataSource[D, P] {
		def read(parameters: P)(implicit spark: SparkSession): AnnotatedDataFrame[D]
	}

	object DataSource {

		def apply[D]: Helper[D] = new Helper[D]

		// Helper is used to improve type inference and make API read more cleanly.
		final class Helper[D] {
			def read[P](parameters: P)(implicit ds: DataSource[D, P],
								  s: SparkSession): AnnotatedDataFrame[D] ={

				ds.read(parameters = parameters)
			}

			def read(implicit ds: DataSource[D, Unit], s: SparkSession): AnnotatedDataFrame[D] =
				ds.read(parameters = ())
		}
	}
}



