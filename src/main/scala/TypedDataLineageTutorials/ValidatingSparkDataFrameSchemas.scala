package TypedDataLineageTutorials

import com.github.mrpowers.spark.daria.sql.DataFrameHelpers.validatePresenceOfColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}

/**
 *
 */
object ValidatingSparkDataFrameSchemas {

	def withFullName()(df: DataFrame): DataFrame = {
		validatePresenceOfColumns(df, Seq("first_name", "last_name")) // todo not sure from which package
		df.withColumn(
			"full_name",
			concat_ws(" ", col("first_name"), col("last_name"))
		)
	}
}
