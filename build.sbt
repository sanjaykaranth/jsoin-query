name := "jsonquery"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.2"
libraryDependencies += "com.lihaoyi" %% "upickle" % "0.6.6"
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.6.6"

libraryDependencies += "com.typesafe.akka" %% "akka-http"   % "10.1.5"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.12" // or whatever the latest version is
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.5"
libraryDependencies += "ch.megard" %% "akka-http-cors" % "0.3.1"