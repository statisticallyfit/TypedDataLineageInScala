package TypedDataLineageTutorials.Iravid_FunctionalProgAndSpark



import scala.util.Random


import cats._, cats.data._, cats.implicits._
import monix.eval.{ Coeval, Task }
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import org.apache.spark.sql.{ functions => f }
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

/**
 * SOURCE = https://github.com/iravid/blog-posts/blob/master/fp-and-spark.org
 *
 */
object part1_UnsafeAPIMotivatingExample extends App {


	implicit val session = (SparkSession
		.builder()
		.appName("Retaining Sanity with Spark")
		.master("local")
		.getOrCreate())

	import session.implicits._


	/**
	 * Simple example - of computing average of a DataFrame of numbers
	 */
	val df1: DataFrame = session.sparkContext.parallelize(List.fill(100)(Random.nextLong)).toDF

	Console.println("Example 1: printing avg of dataframe of numbers: " + df1.agg(f.avg("value")).head())

	// ---------------------------------------------------------------

	/**
	 * What happens when we want to run two of these operations in parallel?
	 * Spark will block when we call `head()` so we need to run this in `Future`
	 *
	 * TODO - why will spark block on calling `head()`??
 	 */
	def computeAvg(df: DataFrame) = Future(df.agg(f.avg("value")).head())

	val df2 = session.sparkContext.parallelize(List.fill(100)(Random.nextLong)).toDF

	val resultOfParallelizing = Await.result(
		awaitable = computeAvg(df1) zip computeAvg(df2),
		atMost = 30.seconds
	)


	// ---------------------------------------------------------------


	/**
	 * Problem with above approach - spark will run the two actions sequentially since the jobs will occupy all cores
	 * available
	 *
	 * Setting `spark.scheduler.mode = FAIR` lets us use thread-locals to run the actions different scheduler pools
	 *
	 * `KEY CONCEPTS` = scheduler pools | sequential | thread local | spark jobs
	 */
	session.stop()

	implicit val session2 = SparkSession
		.builder()
		.appName("Retaining sanity with Spark")
		.master("local")
		.config("spark.scheduler.mode", "FAIR")
		.getOrCreate()

	def computeAvg(df: DataFrame, pool: String)(implicit session: SparkSession) =
		Future {
			session.sparkContext.setLocalProperty(key = "spark.scheduler.pool", value = pool)
			df.agg(f.avg("value")).head()
			session.sparkContext.setLocalProperty(key = "spark.scheduler.pool", value = null)
			// TODO meaning of setting these properties?
			//TODO meaning of pool vs. null value?
		}

	/**
	 * There are now two levels of execution here -
	 * 	1) the `ExecutionContext` on which the `Future` is being executed, and
	 * 	2) the scheduler pool on which the job is being executed
	 *
	 * Cannot use this approach in real life though because this is done using thread-locals and the `f` thunk might
	 * be executed on a different thread entirely.
	 *
	 * TODO: scheduler pool vs. thread vs. thread-local ?
	 */

	// ---------------------------------------------------------------

	// ---------------------------------------------------------------


}
