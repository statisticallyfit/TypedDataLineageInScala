package TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer


import TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer.TypeJoinList._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.broadcast
import shapeless.::
import shapeless.ops.hlist.Prepend

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
		   )(implicit evPrepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: Prepend[LJ, RJ]#Out] //evPrepend.Out]
}

// type PrependOut: Prepend[LJ, RJ]#Out = evPrepend.Out


object Join {
	def apply[L, LJ <: JoinList, R, RJ <: JoinList](joinExprs: Column,
										   joinType: JoinType = Inner,
										   isBroadcast: Boolean = false
										  ): Join[L, LJ, R, RJ] = new Join[L, LJ, R, RJ] {

		override def join(left: AnnotatedDataFrame[L, LJ],
					   right: AnnotatedDataFrame[R, RJ]
					  )(implicit evPrepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: Prepend[LJ,RJ]#Out] =
		///AnnotatedDataFrame[L, R :: evPrepend.Out] =

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
					  )(implicit evPrepend: Prepend[LJ, RJ]): AnnotatedDataFrame[L, R :: Prepend[LJ,RJ]#Out] = //evPrepend.Out] =
			AnnotatedDataFrame(toDF =
				left.toDF.join(
					right =if(isBroadcast) broadcast(right.toDF) else right.toDF,
					usingColumns = joinKeys,
					joinType = joinType.sparkName
				))
	}

	sealed abstract class JoinType(val sparkName: String)
	case object Inner extends JoinType("inner")
	case object LeftOuter extends JoinType("left_outer")
	case object FullOuter extends JoinType("full_outer")
}
