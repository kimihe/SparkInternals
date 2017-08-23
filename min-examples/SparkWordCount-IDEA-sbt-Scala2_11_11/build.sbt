name := "SparkWordCount-IDEA-sbt-Scala2_11_11"

version := "1.0"

scalaVersion := "2.11.11"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
)