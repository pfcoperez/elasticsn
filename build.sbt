name := "securitynow_on_elastic"

version := "0.1"

scalaVersion := "2.12.3"

lazy val elastic4sVersion = "5.6.0"

scalacOptions += "-Ypartial-unification"

libraryDependencies ++= Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,  
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-circe" % elastic4sVersion,
  "com.github.pureconfig" %% "pureconfig" % "0.8.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6",
  "org.slf4j" % "slf4j-simple" % "1.7.25"
)
