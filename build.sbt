ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "hybrid-json-datasource-v1",
    libraryDependencies ++= Dependencies.All
  )
