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

	/**
	 * Each dataframe instance needs a "type tag" to indicate a source from which the instance originates.
	 *
	 *
	 * @param toDF
	 * @tparam D
	 * @tparam J = carries list of joined datasets
	 */
	case class AnnotatedDataFrame[D, J <: JoinList](toDF: DataFrame) extends Serializable


	// Need way to produce annotated dataframe instances - create type class DataSource.
	// TODO - meaning of params D, P?
	// TODO -- mean of D == dataset parameters?
	// TODO -- meag of P == custom source parameters? (https://hyp.is/khcmoDTzEe2iyyMb-Wl4_Q/itnext
	//  .io/making-the-spark-dataframe-composition-type-safe-r-7b6fed524ec2)

	trait DataSource[D, P] {
		def read(parameters: P)(implicit spark: SparkSession): AnnotatedDataFrame[D, HNil]
	}

	object DataSource {

		def apply[D]: Helper[D] = new Helper[D]

		// Helper is used to improve type inference and make API read more cleanly.
		final class Helper[D] {
			def read[P](parameters: P)(implicit ds: DataSource[D, P],
								  s: SparkSession): AnnotatedDataFrame[D, HNil] ={

				ds.read(parameters = parameters)
			}

			def read(implicit ds: DataSource[D, Unit], s: SparkSession): AnnotatedDataFrame[D, HNil] =
				ds.read(parameters = ())
		}
	}

	/**
	 * Join type class
	 *
	 *
	 * Since we model our data sets as type parameters we're going to need another type class to describe how any 2
	 * datasets can be joined.
	 *
	 * Composition of joins should be left-oriented
	 *
	 *
	 * Maintain a heterogeneous list of joined datasets, store as a type (using the type property info from HList,
	 * not its values now)
	 *
	 * Ensuring lineage of joinedtasets
	 * 	1) concatenate join lists of left and right datasets
	 * 	2) prepend type tag of right dataset to the list obtained in the previous step
	 *
	 * `Prepend` = typeclass from shapeless which prepends the HList from the first type parameter (LJ) to the one
	 * in the second type parameter (RJ). The type resulting from this concatenation is store in the path-dependent
	 * type `Out`.
	 * Used to prepend type `R` to type `prepend.Out` (where `prepend.Out` is the type from concatenating `LJ` and
	 * `RJ`.
	 *
	 * @tparam L
	 * @tparam LJ = join list of left dataset
	 * @tparam R
	 * @tparam RJ = join list of right dataset
	 */
	sealed trait Join[L, LJ <: JoinList, R, RJ <: JoinList] {
		def join(left: AnnotatedDataFrame[L, LJ],
			    right: AnnotatedDataFrame[R, RJ]
			   )(implicit prepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: prepend.Out]
	}


	object Join {
		def apply[L, LJ <: JoinList, R, RJ <: JoinList](joinExprs: Column,
											   joinType: JoinType = Inner,
											   isBroadcast: Boolean = false
											  ): Join[L, LJ, R, RJ] = new Join[L, LJ, R, RJ] {

			override def join(left: AnnotatedDataFrame[L, LJ],
						   right: AnnotatedDataFrame[R, RJ]
						  )(implicit prepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: prepend.Out] =

				AnnotatedDataFrame(toDF =
					left.toDF.join(
						right = if(isBroadcast) broadcast(right.toDF) else right.toDF,
						joinExprs = joinExprs,
						joinType = joinType.sparkName
					)
				)
		}

		def usingColumns[L, LJ <: JoinList, R, RJ <: JoinList](joinKeys: Seq[String],
													joinType: JoinType = Inner,
													isBroadcast: Boolean = false
												    ): Join[L, LJ, R, RJ] = new Join[L, LJ, R, RJ] {

			override def join(left: AnnotatedDataFrame[L, LJ],
						   right: AnnotatedDataFrame[R, RJ]
						  )(implicit prepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: prepend.Out] =
				AnnotatedDataFrame(toDF =
					left.toDF.join(
						right =if(isBroadcast) broadcast(right.toDF) else right.toDF,
						usingColumns =joinKeys,
						joinType = joinType.sparkName
					))
		}

		sealed abstract class JoinType(val sparkName: String)
		case object Inner extends JoinType("inner")
		class object LeftOuter extends JoinType("left_outer")
		class object FullOuter extends JoinType("full_outer")
	}


	/**
	 * Transform type class
	 */



	/**
	 * Syntax for AnnotatedDataFrame
	 */
	object implicits {


		//Extending the `AnnatedDataFrame` with a join syntax
		implicit class AnnotatedDataFrameSyntax[L, LJ <: JoinList](left: AnnotatedDataFrame[L, LJ]) {


			// TODO fix type here somehow?
			def join[R, RJ <: JoinList](right: AnnotatedDataFrame[R, RJ] )(
				implicit j: Join[L, LJ, R, RJ], p: Prepend[LJ, RJ]
			): AnnotatedDataFrame[L, R :: p.Out] =
				j.join(left, right)
		}
	}


}




object Example {

	import flow._
	import flow.implicits._



	implicit val spark: SparkSession = ??? // so that can declare the devicemodel / devicemeasure objects at bottom


	/// Defining some data sets to model our data

	/**
	 * DeviceModel = a dimension dataset - each record contains detailed information about each known device model
	 * (model name, hardware specification, etc). Primary key in this dataset is device_model_id
	 */
	sealed trait DeviceModel

	object DeviceModel {
		implicit val deviceModelDataSource = new DataSource[DeviceModel, Unit] {

			override def read(parameters: Unit)(implicit spark: SparkSession) =
				AnnotatedDataFrame[DeviceModel] (toDF =
					spark.createDataFrame(Seq(
						(0, "model_0"),
						(1, "model_1")
					))
						.toDF("device_model_id", "model_name")
				)
		}


		/**
		  * To show the idea behind lineage tracking: must implement the Join type class to describe how
		 * `DeviceMeasurement` and `DeviceModel` datasets should be joined.
		 * Have here a type class instance
		 */
		implicit def deviceModelToMeasurementJoin[LJ <: JoinList, RJ <: JoinList] =
			Join.usingColumns
	}

	/**
	 * DeviceMeasurement - a fact dataset. Each record represents an individual measurement from
	 * and includes the `device_model_id` attribute which is a foreign key used to reverse the `DeviceModel` dataset.
	 */
	sealed trait DeviceMeasurement

	object DeviceMeasurement {

		implicit val deviceMeasurementDataSource = new DataSource[DeviceMeasurement, Unit] {

			override def read(parameters: Unit)(implicit spark: SparkSession) =
				AnnotatedDataFrame[DeviceMeasurement](
					spark.createDataFrame(Seq(
						(0, 1.0),
						(0, 2.0),
						(1, 3.0)
					))
						.toDF("device_model_id", "measurement_value")
				)
		}
	}



	// Example: instantiating the two datasets
	val deviceModel: AnnotatedDataFrame[DeviceModel] = DataSource[DeviceModel].read

	val deviceMeasurement: AnnotatedDataFrame[DeviceMeasurement] = DataSource[DeviceMeasurement].read


	// Example: composability
	/*val country: AnnotatedDataFrame[Country, HNil] =
		DataSource[Country].read

	val joined: AnnotatedDataFrame[DeviceMeasurement, Country :: DeviceModel :: HNil] =
		deviceMeasurement.join(deviceModel).join(country)*/

}
