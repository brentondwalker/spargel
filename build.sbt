name         := "spargel"
version      := "1.0"
organization := "ikt"
scalaVersion := "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-core"  % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql"   % "2.1.1"
//libraryDependencies += "commons-cli" % "commons-cli" % "1.2" % "provided"
//libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
libraryDependencies += "org.vegas-viz" % "vegas-spark_2.11" % "0.3.11"
libraryDependencies += "org.vegas-viz" % "vegas-macros_2.11" % "0.3.11"

resolvers += Resolver.mavenLocal

resolvers	+= Resolver.mavenLocal
// fork in run := true
