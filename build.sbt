ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "hybrid-json-datasource",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core"         % "2.4.8",
      "org.apache.spark"  %% "spark-sql"          % "2.4.8" % "provided",
      "org.scalatest"     %% "scalatest"          % "3.2.9",
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"
    )
  )
