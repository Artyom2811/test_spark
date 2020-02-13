name := "Spark_Scala"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1" //% "provided"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.1" //% "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.11"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.7"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"