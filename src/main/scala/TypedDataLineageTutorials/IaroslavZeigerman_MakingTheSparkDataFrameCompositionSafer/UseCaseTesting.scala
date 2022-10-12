package TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer

import org.apache.spark.sql.SparkSession
import shapeless.HNil


/**
 *
 */

object UseCaseTesting extends App  {

	import TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer.TypeJoinList._
	import TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer.{AnnotatedDataFrame,
		DataSource, Join, Transform}
	import TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer.implicits._



	implicit val spark: SparkSession = (SparkSession
		.builder()
		.appName("AnnotatedDataFrame Example")
		.master("local")
		.getOrCreate())

	//import spark.sqlContext.implicits._
	import spark.implicits._


	/// Defining some data sets to model our data

	/**
	 * DeviceModel = a dimension dataset - each record contains detailed information about each known device model
	 * (model name, hardware specification, etc). Primary key in this dataset is device_model_id
	 */
	sealed trait DeviceModel

	object DeviceModel {
		implicit val deviceModelDataSource: DataSource[DeviceModel, Unit] = new DataSource[DeviceModel, Unit] {

			override def read(parameters: Unit)(implicit evSpark: SparkSession) =
				AnnotatedDataFrame[DeviceModel, HNil] (toDF =
					evSpark.createDataFrame(Seq(
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
		implicit def deviceModelToMeasurementJoin[LJ <: JoinList, RJ <: JoinList]: Join[DeviceMeasurement, LJ, DeviceModel, RJ] =
			Join.usingColumns[DeviceMeasurement, LJ, DeviceModel, RJ](
				joinKeys = Seq("device_model_id")
			)
	}

	/**
	 * DeviceMeasurement - a fact dataset. Each record represents an individual measurement from
	 * and includes the `device_model_id` attribute which is a foreign key used to reverse the `DeviceModel` dataset.
	 */
	sealed trait DeviceMeasurement

	object DeviceMeasurement {

		implicit val deviceMeasurementDataSource = new DataSource[DeviceMeasurement, Unit] {

			override def read(parameters: Unit)(implicit evSpark: SparkSession) =
				AnnotatedDataFrame[DeviceMeasurement, HNil](
					evSpark.createDataFrame(Seq(
						(0, 1.0),
						(0, 2.0),
						(1, 3.0)
					))
						.toDF("device_model_id", "measurement_value")
				)
		}
	}





	//-----------------------------------------------------------------------------------------------------------


	// Example: instantiating the two datasets
	val deviceModel: AnnotatedDataFrame[DeviceModel, HNil] = DataSource[DeviceModel].read
	Console.println(deviceModel)

	val deviceMeasurement: AnnotatedDataFrame[DeviceMeasurement, HNil] = DataSource[DeviceMeasurement].read
	Console.println(deviceMeasurement)

	// TODO why don't these print out? want to see how the objects look like??


	// Example: composability
	/*val country: AnnotatedDataFrame[Country, HNil] =
		DataSource[Country].read

	val joined: AnnotatedDataFrame[DeviceMeasurement, Country :: DeviceModel :: HNil] =
		deviceMeasurement.join(deviceModel).join(country)*/


}