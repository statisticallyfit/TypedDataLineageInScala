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
object Part3_MonixControlExecution_Coeval {

	// Recap problem (part 2): if task has an asynchronous boudnary (like the one produced by `Task.apply`) then
	// the  `setLocalProperty` will set the thread-local on an irrelevant thread


	// Possible solution: to use `monix.eval.Coeval` - data type that represents synchronous evaluation.
	// Coeval has a Monad instance so we can create a Coeval that wraps our spark API call, decorate it with setting
	// / clearing the scheduler pool thread/local and convert the resulting Coeval to a Task.

}