import sbt._

object Dependencies {

  private object Version {}

  private object Spark {
    val Core = "org.apache.spark" %% "spark-core" % "2.4.8"
    val Sql  = "org.apache.spark" %% "spark-sql"  % "2.4.8" % "provided"

    val All: Seq[ModuleID] = Seq(Core, Sql)
  }

  private object Test {
    val ScalaTest = "org.scalatest" %% "scalatest" % "3.2.15"

    val All: Seq[ModuleID] = Seq(ScalaTest)
  }

  private object Mongo {
    val Driver = "org.mongodb.scala" %% "mongo-scala-driver" % "4.9.0"

    val All: Seq[ModuleID] = Seq(Driver)
  }

  val All: Seq[sbt.ModuleID] = Spark.All ++ Test.All ++ Mongo.All

}
