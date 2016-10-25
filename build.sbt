name := "flink-example"

organization := "net.pusuo"

version := "1.0"

scalaVersion := "2.11.8"

val flinkVersion = "1.1.3" //  "1.2-SNAPSHOT"

libraryDependencies ++= Seq(
  //  "org.apache.calcite" % "calcite-core" % "1.7.0" withSources(),
  "org.apache.flink" % "flink-core" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-table" % flinkVersion withSources(),
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion withSources()
)

//assemblyMergeStrategy in assembly := {
//  case PathList("about.html") => MergeStrategy.rename
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}

//resolvers += "Apache Snapshots" at "https://repository.apache.org/content/groups/snapshots"
