name := "poc"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies := Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0", // % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.4.0", // % "provided",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"
)