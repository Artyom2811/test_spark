name := "scala_kafka"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.0"
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.13"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7"
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6"