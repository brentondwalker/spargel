name         := "spargel"
version      := "2.0"
organization := "ikt"
scalaVersion := "2.12.10"

/**
  * Makes it possible to run the application from sbt-shell.
  * Otherwise java.lang.InterruptedException gets called and
  * the context isn't closed correctly
  */
fork         := true

libraryDependencies += "org.apache.spark" %% "spark-core"  % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "3.0.1"
//libraryDependencies += "org.vegas-viz" % "vegas-spark_2.11" % "0.3.11"
//libraryDependencies += "org.vegas-viz" % "vegas-macros_2.11" % "0.3.11"
libraryDependencies += "commons-cli" % "commons-cli" % "1.4"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"


//resolvers	+= Resolver.mavenLocal
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
// fork in run := true

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
