name         := "spargel"
version      := "4.0"
organization := "ikt"
scalaVersion := "2.13.17"

/**
  * Makes it possible to run the application from sbt-shell.
  * Otherwise java.lang.InterruptedException gets called and
  * the context isn't closed correctly
  */
fork         := true

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.1.0-preview4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.0-preview4" % "provided"
libraryDependencies += "commons-cli" % "commons-cli" % "1.5.0"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"


//resolvers	+= Resolver.mavenLocal
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
// fork in run := true

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
