package TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer

import TypedDataLineageTutorials.IaroslavZeigerman_MakingTheSparkDataFrameCompositionSafer.TypeJoinList.JoinList
/*flow
.{AnnotatedDataFrame,	Join, JoinList, Transform}*/
import org.apache.spark.sql.SparkSession
import shapeless.{::, HNil}
import shapeless.ops.hlist.Prepend

/**
 *
 */

/**
 * Syntax for AnnotatedDataFrame
 */
object implicits {


	//Extending the `AnnatedDataFrame` with a join syntax
	implicit class AnnotatedDataFrameSyntax[L, LJ <: JoinList](left: AnnotatedDataFrame[L, LJ]) {


		// TODO why does it say
		//		 Found: evPrepend.Out
		//		 Required: Prepend[LJ, RJ]#Out ?????? What is the difference / meaning?
		// NOTE answer = hashtag # is to access the type at TYPE-LEVEL whereas the dot operator is to access the type at VALUE-LEVEL
		// SOURCES
		// 		https://hyp.is/0uGTgjjqEe2R-qOLCl5FFA/stackoverflow.com/questions/2498779/accessing-type-members-outside-the-class-in-scala
		//		https://hyp.is/YerV6DjrEe2lir8bbZaziA/blog.rockthejvm.com/scala-3-type-projections/
		// 		https://hyp.is/Iq8s0jjzEe2nTu85E4u5mg/typelevel.org/blog/2015/07/23/type-projection.html
		// 		(TODO suggestion to move to componion object instead of using #)
		// 		meaning of # vs. dot = https://hyp.is/aLpw2DjzEe2gHiuCbFWUdA/stackoverflow.com/questions/16471318/scala-type-projection
		// 		pg. 387 Dean Wampler Programming Scala
		def join[R, RJ <: JoinList](right: AnnotatedDataFrame[R, RJ] )
							  (implicit evJoin: Join[L, LJ, R, RJ],
							   evPrepend: Prepend[LJ, RJ]
							  ): AnnotatedDataFrame[L, R :: Prepend[LJ, RJ]#Out]
		= evJoin.join(left, right)


		def transform[R, P](parameters: P)(implicit evTransf: Transform[L, LJ, R, P],
									evSpark: SparkSession): AnnotatedDataFrame[R, HNil] =
			evTransf.transform(left, parameters)


		// NOTE:    P = Unit (no parameters)
		def transform[R](implicit evTransf: Transform[L, LJ, R, Unit],
					  evSpark: SparkSession): AnnotatedDataFrame[R, HNil] =
			evTransf.transform(left, ())

	}

}