name := "flink-example"

organization := "net.pusuo"

version := "1.0"

scalaVersion := "2.11.8"

val flinkVersion = "1.1.2"

libraryDependencies ++= Seq(
  "org.apache.calcite" % "calcite-core" % "1.7.0" withSources(),
  "org.apache.flink" % "flink-core" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-table" % flinkVersion withSources()
)
