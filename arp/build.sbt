name := "ARPscala"

version := "0.1"

scalaVersion := "2.11.8"

organization := "Semantix"

resolvers ++= Seq(
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"

libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

//assemblyJarName in assembly := "semantix-belcorp.jar"
