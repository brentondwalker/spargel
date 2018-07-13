name         := "spargel"
version      := "1.0"
organization := "ikt"
scalaVersion := "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-core"  % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "2.1.1"
//libraryDependencies += "org.apache.spark" % "spark-mllib-local_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.1" % "provided"
libraryDependencies += "org.vegas-viz" % "vegas-spark_2.11" % "0.3.11"
libraryDependencies += "org.vegas-viz" % "vegas-macros_2.11" % "0.3.11"

resolvers	+= Resolver.mavenLocal
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
// fork in run := true

