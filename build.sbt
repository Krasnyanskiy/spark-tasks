name := "Spark Tasks"

scalaVersion := "2.11.12"
scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "junit" % "junit" % "4.10" % Test

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3"
)