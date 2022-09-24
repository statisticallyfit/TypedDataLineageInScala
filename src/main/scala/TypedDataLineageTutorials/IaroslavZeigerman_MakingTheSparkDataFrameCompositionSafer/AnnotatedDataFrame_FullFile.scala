//package TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer
//
//
//import org.apache.spark.sql.{Column, DataFrame, SparkSession}
//import org.apache.spark.sql.functions.broadcast
//import shapeless.ops.hlist.Prepend
//import shapeless.{::, HList, HNil}
//
//
///**
// * Tutorial source = https://itnext.io/making-the-spark-dataframe-composition-type-safe-r-7b6fed524ec2
// */
//
//object flow {
//	type JoinList = HList
//
//	/**
//	 * Each dataframe instance needs a "type tag" to indicate a source from which the instance originates.
//	 *
//	 *
//	 * @param toDF
//	 * @tparam D
//	 * @tparam J = carries list of joined datasets
//	 */
//	case class AnnotatedDataFrame[D, +J <: JoinList](toDF: DataFrame) extends Serializable
//
//	/**
//	 * NOTE: changed from type `J` to `+J` because got eror saying:
//	 *
//	 * Note: R :: evPrepend.Out <: R :: shapeless.ops.hlist.Prepend[LJ,RJ]#Out, but class AnnotatedDataFrame is invariant in type J.
//	 * You may wish to define J as +J instead. (SLS 4.5)
//			= evJoin.join(left, right)
//	 */
//
//
//	/**
//	 * Need way to produce annotated dataframe instances - create type class DataSource.
//	 * @tparam D
//	 * @tparam P
//	 */
//	// TODO - meaning of params D, P?
//	// TODO -- mean of D == dataset parameters?
//	// TODO -- meag of P == custom source parameters? (https://hyp.is/khcmoDTzEe2iyyMb-Wl4_Q/itnext
//	//  .io/making-the-spark-dataframe-composition-type-safe-r-7b6fed524ec2)
//
//	trait DataSource[D, P] {
//		def read(parameters: P)(implicit evSpark: SparkSession): AnnotatedDataFrame[D, HNil]
//	}
//
//	object DataSource {
//
//		def apply[D]: Helper[D] = new Helper[D]
//
//		// Helper is used to improve type inference and make API read more cleanly.
//		final class Helper[D] {
//			def read[P](parameters: P)(implicit evDataSrc: DataSource[D, P],
//								  evSpark: SparkSession): AnnotatedDataFrame[D, HNil] ={
//
//				evDataSrc.read(parameters = parameters)
//			}
//
//			def read(implicit evDataSrc: DataSource[D, Unit], s: SparkSession): AnnotatedDataFrame[D, HNil] =
//				evDataSrc.read(parameters = ())
//		}
//	}
//
//	/**
//	 * Join type class
//	 *
//	 *
//	 * Since we model our data sets as type parameters we're going to need another type class to describe how any 2
//	 * datasets can be joined.
//	 *
//	 * Composition of joins should be left-oriented
//	 *
//	 *
//	 * Maintain a heterogeneous list of joined datasets, store as a type (using the type property info from HList,
//	 * not its values now)
//	 *
//	 * Ensuring lineage of joinedtasets
//	 * 	1) concatenate join lists of left and right datasets
//	 * 	2) prepend type tag of right dataset to the list obtained in the previous step
//	 *
//	 * `Prepend` = typeclass from shapeless which prepends the HList from the first type parameter (LJ) to the one
//	 * in the second type parameter (RJ). The type resulting from this concatenation is store in the path-dependent
//	 * type `Out`.
//	 * Used to prepend type `R` to type `prepend.Out` (where `prepend.Out` is the type from concatenating `LJ` and
//	 * `RJ`.
//	 *
//	 * @tparam L
//	 * @tparam LJ = join list of left dataset
//	 * @tparam R
//	 * @tparam RJ = join list of right dataset
//	 */
//	sealed trait Join[L, LJ <: JoinList, R, RJ <: JoinList] {
//		def join(left: AnnotatedDataFrame[L, LJ],
//			    right: AnnotatedDataFrame[R, RJ]
//			   )(implicit evPrepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: Prepend[LJ, RJ]#Out] //evPrepend.Out]
//	}
//
//
//	object Join {
//		def apply[L, LJ <: JoinList, R, RJ <: JoinList](joinExprs: Column,
//											   joinType: JoinType = Inner,
//											   isBroadcast: Boolean = false
//											  ): Join[L, LJ, R, RJ] = new Join[L, LJ, R, RJ] {
//
//			override def join(left: AnnotatedDataFrame[L, LJ],
//						   right: AnnotatedDataFrame[R, RJ]
//						  )(implicit evPrepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: Prepend[LJ,RJ]#Out] =
//			///AnnotatedDataFrame[L, R :: evPrepend.Out] =
//
//				AnnotatedDataFrame(toDF =
//					left.toDF.join(
//						right = if(isBroadcast) broadcast(right.toDF) else right.toDF,
//						joinExprs = joinExprs,
//						joinType = joinType.sparkName
//					)
//				)
//		}
//
//		def usingColumns[L, LJ <: JoinList, R, RJ <: JoinList](joinKeys: Seq[String],
//													joinType: JoinType = Inner,
//													isBroadcast: Boolean = false
//												    ): Join[L, LJ, R, RJ] = new Join[L, LJ, R, RJ] {
//
//			override def join(left: AnnotatedDataFrame[L, LJ],
//						   right: AnnotatedDataFrame[R, RJ]
//						  )(implicit evPrepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: Prepend[LJ,RJ]#Out] = //evPrepend.Out] =
//				AnnotatedDataFrame(toDF =
//					left.toDF.join(
//						right =if(isBroadcast) broadcast(right.toDF) else right.toDF,
//						usingColumns = joinKeys,
//						joinType = joinType.sparkName
//					))
//		}
//
//		sealed abstract class JoinType(val sparkName: String)
//		case object Inner extends JoinType("inner")
//		case object LeftOuter extends JoinType("left_outer")
//		case object FullOuter extends JoinType("full_outer")
//	}
//
//
//	/**
//	 * Transform type class
//	 */
//	trait Transform[I, IJ <: JoinList, O, P] {
//
//		def transform(input: AnnotatedDataFrame[I, IJ], parameters: P)
//				   (implicit evSpark: SparkSession): AnnotatedDataFrame[O, HNil]
//	}
//
//
//
//	/**
//	 * Syntax for AnnotatedDataFrame
//	 */
//	object implicits {
//
//
//		//Extending the `AnnatedDataFrame` with a join syntax
//		implicit class AnnotatedDataFrameSyntax[L, LJ <: JoinList](left: AnnotatedDataFrame[L, LJ]) {
//
//
//			// TODO why does it say
//			//		 Found: evPrepend.Out
//			//		 Required: Prepend[LJ, RJ]#Out ?????? What is the difference / meaning?
//			// NOTE answer = hashtag # is to access the type at TYPE-LEVEL whereas the dot operator is to access the type at VALUE-LEVEL
//			// SOURCES
//			// 		https://hyp.is/0uGTgjjqEe2R-qOLCl5FFA/stackoverflow.com/questions/2498779/accessing-type-members-outside-the-class-in-scala
//			//		https://hyp.is/YerV6DjrEe2lir8bbZaziA/blog.rockthejvm.com/scala-3-type-projections/
//			// 		https://hyp.is/Iq8s0jjzEe2nTu85E4u5mg/typelevel.org/blog/2015/07/23/type-projection.html
//			// 		(TODO suggestion to move to componion object instead of using #)
//			// 		meaning of # vs. dot = https://hyp.is/aLpw2DjzEe2gHiuCbFWUdA/stackoverflow.com/questions/16471318/scala-type-projection
//			// 		pg. 387 Dean Wampler Programming Scala
//			def join[R, RJ <: JoinList](right: AnnotatedDataFrame[R, RJ] )
//								  (implicit evJoin: Join[L, LJ, R, RJ],
//								   evPrepend: Prepend[LJ, RJ]
//								  ): AnnotatedDataFrame[L, R :: Prepend[LJ, RJ]#Out]
//			= evJoin.join(left, right)
//
//
//			def transform[R, P](parameters: P)(implicit evTransf: Transform[L, LJ, R, P],
//										evSpark: SparkSession): AnnotatedDataFrame[R, HNil] =
//				evTransf.transform(left, parameters)
//
//
//			// NOTE:    P = Unit (no parameters)
//			def transform[R](implicit evTransf: Transform[L, LJ, R, Unit],
//						 evSpark: SparkSession): AnnotatedDataFrame[R, HNil] =
//				evTransf.transform(left, ())
//
//		}
//
//	}
//
//
//}
//
//
//
//
//object UseCaseTesting extends App  {
//
//	import flow._
//	import flow.implicits._
//
//
//
//	implicit val spark: SparkSession = (SparkSession
//		.builder()
//		.appName("AnnotatedDataFrame Example")
//		.master("local")
//		.getOrCreate())
//
//	//import spark.sqlContext.implicits._
//	import spark.implicits._
//
//
//	/// Defining some data sets to model our data
//
//	/**
//	 * DeviceModel = a dimension dataset - each record contains detailed information about each known device model
//	 * (model name, hardware specification, etc). Primary key in this dataset is device_model_id
//	 */
//	sealed trait DeviceModel
//
//	object DeviceModel {
//		implicit val deviceModelDataSource: DataSource[DeviceModel, Unit] = new DataSource[DeviceModel, Unit] {
//
//			override def read(parameters: Unit)(implicit evSpark: SparkSession) =
//				AnnotatedDataFrame[DeviceModel, HNil] (toDF =
//					evSpark.createDataFrame(Seq(
//						(0, "model_0"),
//						(1, "model_1")
//					))
//						.toDF("device_model_id", "model_name")
//				)
//		}
//
//
//		/**
//		  * To show the idea behind lineage tracking: must implement the Join type class to describe how
//		 * `DeviceMeasurement` and `DeviceModel` datasets should be joined.
//		 * Have here a type class instance
//		 */
//		implicit def deviceModelToMeasurementJoin[LJ <: JoinList, RJ <: JoinList]: Join[DeviceMeasurement, LJ, DeviceModel, RJ] =
//			Join.usingColumns[DeviceMeasurement, LJ, DeviceModel, RJ](
//				joinKeys = Seq("device_model_id")
//			)
//	}
//
//	/**
//	 * DeviceMeasurement - a fact dataset. Each record represents an individual measurement from
//	 * and includes the `device_model_id` attribute which is a foreign key used to reverse the `DeviceModel` dataset.
//	 */
//	sealed trait DeviceMeasurement
//
//	object DeviceMeasurement {
//
//		implicit val deviceMeasurementDataSource = new DataSource[DeviceMeasurement, Unit] {
//
//			override def read(parameters: Unit)(implicit evSpark: SparkSession) =
//				AnnotatedDataFrame[DeviceMeasurement, HNil](
//					evSpark.createDataFrame(Seq(
//						(0, 1.0),
//						(0, 2.0),
//						(1, 3.0)
//					))
//						.toDF("device_model_id", "measurement_value")
//				)
//		}
//	}
//
//
//	//-----------------------------------------------------------------------------------------------------------
//
//
//	// Example: instantiating the two datasets
//	val deviceModel: AnnotatedDataFrame[DeviceModel, HNil] = DataSource[DeviceModel].read
//	Console.println(deviceModel)
//
//	val deviceMeasurement: AnnotatedDataFrame[DeviceMeasurement, HNil] = DataSource[DeviceMeasurement].read
//	Console.println(deviceMeasurement)
//
//	// TODO why don't these print out? want to see how the objects look like??
//
//
//	// Example: composability
//	/*val country: AnnotatedDataFrame[Country, HNil] =
//		DataSource[Country].read
//
//	val joined: AnnotatedDataFrame[DeviceMeasurement, Country :: DeviceModel :: HNil] =
//		deviceMeasurement.join(deviceModel).join(country)*/
//
//
//}
