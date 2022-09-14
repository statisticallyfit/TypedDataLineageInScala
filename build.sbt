name := "TypedDataLineageInScala"

version := "0.1"

scalaVersion := "2.12.13"



logLevel := Level.Info

// disable printing timing information, but still print [success]
//showTiming := false

//added here when suggested by sbt in command line
//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// disable printing a message indicating the success or failure of running a task
//showSuccess := false

// append -deprecation to the options passed to the Scala compiler
scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-language:postfixOps", "-language:higherKinds", "-Ypartial-unification")

// TOOD need these?
//"-language:existentials",
//     "-language:higherKinds",
//     "-language:implicitConversions",

// disable updating dynamic revisions (including -SNAPSHOT versions)
offline := true


//For the Kind projector plugin
resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.10")


libraryDependencies ++= Seq(
	//////////////"org.apache.commons" % "commons-lang3" % "3.12.0", // was 3.6
	//Scala Reflections
	"org.scala-lang" % "scala-reflect" % "2.12.13",
	//ScalaCheck
	"org.scalacheck" %% "scalacheck" % "1.15.4" % Test, // was 1.15.2
	// ScalaTest
	"org.scalatest" %% "scalatest" % "3.2.10" % Test,
	// ScalaTestPlus (property-based mixed with specs-based testing)
	"org.scalatestplus" %% "scalacheck-1-15" % "3.2.10.0" % Test,
	//Specs2
	"org.specs2" %% "specs2-core" % "4.12.9" % Test, // was 4.10.6
	"org.specs2" %% "specs2-scalacheck" % "4.12.9" % Test, // was 4.10.6
	//Discipline
	"org.typelevel" %% "discipline" % "0.11.1", //was 0.8
	//Spire
	//////////////"org.typelevel" %% "spire" % "0.17.0", // was 0.16.2
	//////////////"org.typelevel" %% "spire-laws" % "0.17.0", /// was 0.16.2
	//Algebra
	//////////////"org.typelevel" %% "algebra" % "2.2.3" % Test, // was 2.0.0
	//////////////"org.typelevel" %% "algebra-laws" % "2.2.3" % Test, // was 2.0.0
	//Scalaz
	"org.scalaz"      %% "scalaz-core"    % "7.3.5", // 7.3.0-M19
	//Cats
	//"org.typelevel"   %% "cats"           % "1.0.1",
	"org.typelevel" %% "cats-core" %        "2.6.1", //was 2.0.0
	"org.typelevel"   %% "cats-macros"           % "2.1.1", // was 2.0.0
	"org.typelevel"   %% "cats-kernel"           % "2.6.1", //was 2.0.0
	"org.typelevel"   %% "cats-laws"           % "2.6.1", // was 2.0.0
	"org.typelevel"   %% "cats-free"           % "2.6.1", // was 2.0.0
	"org.typelevel"   %% "cats-testkit"           % "2.6.1", // was 2.0.0


	//Shapeless
	"com.chuusai"     %% "shapeless"      % "2.3.7", // was 2.3.3

	//Kind projector plugin
	"org.spire-math" %% "kind-projector" % "0.9.10", // was 0.9.9

	//FunctionMeta library to print function name, arguments... for a function while inside that function
	////////////"com.github.katlasik" %% "functionmeta" % "0.4.1" % "provided",
	// sourcecode library to print function name passed as argument
	////////////"com.lihaoyi" %% "sourcecode" % "0.2.7",

	// Matryoshka
	"com.slamdata" %% "matryoshka-core" % "0.21.3",

	//Droste recursion schemes
	"io.higherkindness" %% "droste-core" % "0.8.0",
	"io.higherkindness" %% "droste-laws" % "0.8.0",
	"io.higherkindness" %% "droste-macros" % "0.8.0",
	/*"io.higherkindness" %% "droste-meta" % "0.8.0",
	"io.higherkindness" %% "droste-reftree" % "0.8.0",*/
	"io.higherkindness" %% "droste-scalacheck" % "0.8.0",

	// Cats Effect
	"org.typelevel" %% "cats-effect" % "2.5.4", //"3.2.8",

	// Monix
	// https://mvnrepository.com/artifact/io.monix/monix
	"io.monix" %% "monix" % "3.3.0", // also loads monix-eval, monix-catnap

	// Spark
	// https://mvnrepository.com/artifact/org.apache.spark/spark-core
	"org.apache.spark" %% "spark-core" % "3.3.0", // spark-core
	"org.apache.spark" %% "spark-sql" % "3.3.0" % "provided", // spark-sql
	"com.databricks" %% "spark-xml" % "0.15.0", // spark-xml - A library for parsing and querying XML data with Apache Spark, for Spark SQL and DataFrames. (provided only by databricks not by Apache)
	"org.apache.spark" %% "spark-avro" % "3.3.0", // spark-avro


	// TODO - other dependencies from the blogs
	// spark-daria - helper plugins for spark
	"com.github.mrpowers" %% "spark-daria" % "1.2.3"


)