name := "DIMSUM_ENSAE"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.2" % "provided"

)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)
