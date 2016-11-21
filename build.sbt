name := "flink-example"
organization := "net.pusuo"
version := "1.0"
scalaVersion := "2.11.8"
//autoScalaLibrary := false

val flinkVersion = "1.2-SNAPSHOT"
//val flinkVersion = "1.1.3"

libraryDependencies ++= Seq(
  //  "org.apache.calcite" % "calcite-core" % "1.9.0" withSources(),
  "org.apache.flink" % "flink-core" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-table" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-gelly" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-gelly-scala" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-clients" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-statebackend-rocksdb" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion withSources(),
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
)

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs@_*) =>
//    xs.map(_.toLowerCase) match {
//      case ("manifest.mf" :: Nil) => MergeStrategy.discard
//    }
//  case x => MergeStrategy.last
//}
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

//resolvers += "Apache Snapshots" at "https://repository.apache.org/content/groups/snapshots/"
//resolvers += "Apache Public" at "https://repository.apache.org/content/groups/public/"
