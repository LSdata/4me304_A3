import AssemblyKeys._

name := "stragefors_A3"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided"

libraryDependencies += "com.google.code.gson" % "gson" % "2.3"

libraryDependencies += "commons-cli" % "commons-cli" % "1.2"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.2"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
