name         := "spargel"
version      := "1.0"
organization := "ikt"
scalaVersion := "2.11.8"

/**
  * Makes it possible to run the application from sbt-shell.
  * Otherwise java.lang.InterruptedException gets called and
  * the context isn't closed correctly
  */
fork         := true

libraryDependencies += "org.apache.spark" %% "spark-core"  % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "2.4.0"
//libraryDependencies += "org.apache.spark" % "spark-mllib-local_2.11" % "2.4.0"
//libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.0" % "provided"
//libraryDependencies += "org.vegas-viz" % "vegas-spark_2.11" % "0.3.11"
//libraryDependencies += "org.vegas-viz" % "vegas-macros_2.11" % "0.3.11"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"


resolvers	+= Resolver.mavenLocal
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
// fork in run := true

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
