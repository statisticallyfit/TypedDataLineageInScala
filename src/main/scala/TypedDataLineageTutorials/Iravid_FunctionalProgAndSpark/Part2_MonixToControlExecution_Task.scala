package TypedDataLineageTutorials.Iravid_FunctionalProgAndSpark


import scala.util.Random
import cats._
import cats.data._
import cats.implicits._
import monix.eval.{Coeval, Task}
import monix.execution.{CancelableFuture, Scheduler}
import monix.execution.Scheduler.Implicits.global
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.{functions => f}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
 *
 */
object Part2_MonixToControlExecution_Task {

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
	def buildSession: SparkSession = SparkSession
		.builder()
		.appName("Retaining sanity with Spark")
		.master("local")
		.config("spark.scheduler.mode", "FAIR")
		.getOrCreate()

	def buildSessionWithTask: Task[SparkSession] = Task.eval { buildSession }

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
			sparkSession: SparkSession <- buildSessionWithTask

			result <- {
				implicit val session: SparkSession = sparkSession
				import scala.util.Random

				val data: List[Int] = List.fill(100)(Random.nextInt)

				for {
					df: DataFrame <- createDF(data) // TODO why isn't this seen?
					avg: Double <- computeAvg(df = df,  poolName ="pool")
				} yield avg
			}
		} yield result
	}
	// NOTE: solved the "withFilter" error and red underline of monadic arrows using this library in sbt = https://hyp.is/3Vo-ijziEe2bHbPD0_VR2A/github.com/oleg-py/better-monadic-for


	/**
	 * The `onPool` combinator = composes the actual task with two other tasks that set / unset the thread-local
	 *
	 * Problem with this naive version = if `task` has an asynchronous boundary (like the one produced by `Task
	 * .apply`) then the `setLocalProperty` will set the thread-local on an irrelevant thread.
	 *
	 * @param task
	 * @param poolName
	 * @param session
	 * @tparam A
	 * @return
	 */
	def onPool[A](task: Task[A], poolName: String)(implicit session: SparkSession): Task[A] = {
		for {
			_ <- Task.eval(session.sparkContext.setLocalProperty(key = "spark.scheduler.pool", value = "pool"))
			result <- task
			_ <- Task.eval(session.sparkContext.setLocalProperty(key = "spark.scheduler.pool", value = null))
		} yield result
	}

}


object Runner2 extends App {

	import Part2_MonixToControlExecution_Task._

	implicit val sessionForOnPoolFunction: SparkSession = buildSession
	val sessionObj: SparkSession = buildSession

	// Works properly for `Task.eval`:

	// NOTE: eval() promotes / lifts a non-strict value to a Task (https://monix.io/api/current/monix/eval/Task$.html#eval[A](a:=%3EA):monix.eval.Task[A])
	val test: Task[Unit] = Task.eval(println(sessionObj.sparkContext.setLocalProperty(key = "spark.scheduler" +
		".pool", 	value = "pool")))

	println(s"test = $test")

	val result_eval: CancelableFuture[Unit] = onPool(task = test, poolName = "pool").runToFuture
	println(s"result_eval = $result_eval")

	// note: runAsync was used here but was deprecated for `runToFuture`

	// but not for Task.fork:
	val forked: Task[Unit] = Task.fork(test)
	val result_fork: CancelableFuture[Unit] = onPool(forked, "pool").runToFuture
	println(s"result_fork = $result_fork")

	// note: fork is deprecated, use executeAsync
	// NOTE: executeAsync mirrors the given Task but upon execution it ensures evaluation forks into a separate
	//  thread = https://monix.io/api/current/monix/eval/Task$.html#eval[A](a:=%3EA):monix.eval.Task[A]

	// TODO understand better the meaning of each function + what should get printed + what is the purpose
	// here + why the first line is "working properly" while the task.fork line is not? https://hyp.is/6rSNfvCYEeye0Fsm-prLdQ/github.com/iravid/blog-posts/blob/master/fp-and-spark.org
}


