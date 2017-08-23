name := "For_Source_Codes_Reading_SparkWordCount-IDEA-sbt-Scala2_11_11"

version := "0.1"

scalaVersion := "2.11.11"

// additional libraries
libraryDependencies ++= Seq(
  //"org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
  "org.apache.spark" %% "spark-core" % "2.2.0" % "compile"
)