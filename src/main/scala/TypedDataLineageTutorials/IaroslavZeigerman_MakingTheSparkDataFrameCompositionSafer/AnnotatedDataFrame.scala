package TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer

import TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer.TypeJoinList.JoinList
import org.apache.spark.sql.DataFrame

/**
 *
 */

/**
 * Each dataframe instance needs a "type tag" to indicate a source from which the instance originates.
 *
 *
 * @param toDF
 * @tparam D
 * @tparam J = carries list of joined datasets
 */
case class AnnotatedDataFrame[D, +J <: JoinList](toDF: DataFrame) extends Serializable

/**
 * NOTE: changed from type `J` to `+J` because got eror saying:
 *
 * Note: R :: evPrepend.Out <: R :: shapeless.ops.hlist.Prepend[LJ,RJ]#Out, but class AnnotatedDataFrame is invariant in type J.
 * You may wish to define J as +J instead. (SLS 4.5)
			= evJoin.join(left, right)
 */
