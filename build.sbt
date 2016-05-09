name := "poc"

version := "1.0"

//scalaVersion in ThisBuild := "2.10.4"

scalaVersion := "2.10.4"

//sovrascrive i scalaVersion values (warning dependencies)
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers += "OpenIMAJ maven releases repository" at "http://maven.openimaj.org"
resolvers += "OpenIMAJ maven snapshots repository" at "http://snapshots.openimaj.org"

libraryDependencies := Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",

  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
  "com.databricks" % "spark-csv_2.10" % "1.2.0",

  //per image classification pipeline
  "org.openimaj" % "core-image" % "1.3.1",
  "org.openimaj" % "core" % "1.3.1",
  "org.openimaj" % "image-feature-extraction" % "1.3.1",
  "org.openimaj" % "image-local-features" % "1.3.1",
  "org.openimaj" % "image-annotation" % "1.3.1"
)