lazy val root = (project in file(".")).
settings (
  name := "Raft",
  version := "1.0",
  scalaVersion := "2.12.5",
  scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation"),
  resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.11",
  libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.5.11"

)
