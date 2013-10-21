name := "druid-scala-aggregations"

organization := "com.tapad"

version := "0.1-SNAPSHOT"

scalaVersion := "2.9.3"

crossScalaVersions := Seq("2.10.2", "2.9.3")

libraryDependencies ++= Seq(
  "io.druid" % "druid-processing" % "0.6.1-SNAPSHOT" changing,
  "io.druid" % "druid-api" % "0.1.2-SNAPSHOT" changing,
  "com.twitter" %% "algebird-core" % "0.2.0",
  "org.scalatest" %% "scalatest" % "1.9.2" % "test",
  "org.joda" % "joda-convert" % "1.5" % "test"
)

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
