import _root_.sbt.Keys._

name := "goju"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "dl-john-ky" at "http://dl.john-ky.io/maven/releases"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.2"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.2"
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.4.2"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "joda-time" % "joda-time" % "2.9.3"
libraryDependencies += "org.joda" % "joda-convert" % "1.8"
libraryDependencies += "com.google.guava" % "guava" % "19.0"
libraryDependencies += "com.github.xuwei-k" %% "msgpack4z-native" % "0.3.0"
libraryDependencies += "com.github.xuwei-k" %% "msgpack4z-core" % "0.3.3"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7"
libraryDependencies += "io.john-ky" %% "hashids-scala" % "1.1.2-2974446"

parallelExecution in Test := false