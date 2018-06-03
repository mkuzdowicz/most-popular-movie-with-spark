
name := "most-popular-movie"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "com.typesafe" % "config" % "1.3.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

resolvers += "maven-repo" at "http://central.maven.org/maven2"
