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
 *
 */
object part2_MonixToControlExecution {


	/**
	 * REGAINING FUNCTIONAL SANITY
	 *
	 * Goals to achieve:
	 * -- use monix to create a limited version of the `onPool` combinator and run spark computations safely and
	 * concurrently
	 * -- measure execution time of each computation
	 * -- abstract over passing around intermediate state
	 */
	// ---------------------------------------------------------------

	/**
	 * USING MONIX TO CONTROL EXECUTION
	 *
	 * First wrapping all spark calls in the `Task.eval` or `Task.apply` constructors
	 *
	 * //TODO task apply = https://monix.io/api/current/monix/eval/Task$.html#apply[A](a:=%3EA):monix.eval.Task[A]
	 *
	 * TODO task eval = https://monix.io/api/current/monix/eval/Task$.html#eval[A](a:=%3EA):monix.eval.Task[A]
	 *
	 * and comapre them
	 */
	def buildSession: Task[SparkSession] = Task.eval {
		SparkSession
			.builder()
			.appName("Retaining sanity with Spark")
			.master("local")
			.config("spark.scheduler.mode", "FAIR")
			.getOrCreate()
	}

	def createDF(data: List[Int])(implicit session: SparkSession): Task[DataFrame] = Task.eval {
		import session.implicits._

		val rdd = session.sparkContext.parallelize(data)

		rdd.toDF
	}

	def computeAvg(df: DataFrame, poolName: String)(implicit session: SparkSession): Task[Double] = {
		Task.eval{
			session.sparkContext.setLocalProperty(key = "spark.scheduler.pool", value = poolName)
			val result: Double = df.agg(f.avg("value")).head().getDouble(0)
			session.sparkContext.setLocalProperty(key = "spark.scheduler.pool", value = null)

			result
		}
	}

	// Now composing everything together lets us get back a `Task` representing the result of our program:
	def program: Task[Double] = {
		for {
			sparkSession: SparkSession <- buildSession
			result <- {
				implicit val session = sparkSession
				import scala.util.Random

				val data: List[Int] = List.fill(100)(Random.nextInt)

				for {
					df: DataFrame <- createDF(data)
					avg: Double <- computeAvg(df = df,  poolName ="pool")
				} yield avg
			}
		} yield result
	}


}
