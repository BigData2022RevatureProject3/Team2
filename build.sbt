ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.10"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"
libraryDependencies += "commons-validator" % "commons-validator" % "1.7"

lazy val root = (project in file("."))
  .settings(
    name := "Team2"
  )