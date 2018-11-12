import scala.io

name := "com.rakuten.dirty_cat"

organization := "RakutenInstitutOfTechnology" 

version := io.Source.fromFile("version.txt").mkString.trim

scalaVersion := "2.11.8"  


developers := List(Developer(id="ahoyosid", name="andres.hoyosidrobo@rakuten.com", email="andres.hoyosidrobo@rakuten.com", url=url("https://github.com/ahoyosid")))


scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")

val sparkVersion = "2.3.0"

resolvers ++= Seq("All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)


licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

libraryDependencies ++= Seq(
  /* "org.apache.xbean" % "" % "", */ 
  "com.google.guava" % "guava" % "23.5-jre",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.holdenkarau" %% "spark-testing-base" %  s"${sparkVersion}_0.10.0" % "test",
  // spark core
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,  
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion, 
  "org.apache.spark" % "spark-catalyst_2.11" % sparkVersion,
  "org.apache.spark" % "spark-mllib_2.11" % sparkVersion 
)

