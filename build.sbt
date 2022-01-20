ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9"

lazy val root = (project in file("."))
  .settings(
    name := "Team2"
  )