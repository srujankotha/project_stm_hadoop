name := "MyProjectv2"

version := "0.1"
libraryDependencies += "com.opencsv" % "opencsv" % "5.1"
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",

).map(_ % hadoopVersion)

scalaVersion := "2.11.0"
val hadoopVersion = "2.7.3"