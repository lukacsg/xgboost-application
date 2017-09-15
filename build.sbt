name := "xgboost-application"

version := "0.1.0"

scalaVersion := "2.11.7"

lazy val javaVersion = "1.8"


//scalacOptions += s"-source:jvm-$javaVersion"
scalacOptions += s"-target:jvm-$javaVersion"

javacOptions ++= Seq("-source", javaVersion)
javacOptions ++= Seq("-target", javaVersion)

compileOrder := CompileOrder.JavaThenScala

resolvers += Resolver.mavenLocal

lazy val commonDependencies = Seq(
  "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.22",
  "org.slf4j" % "slf4j-log4j12" % "1.7.22",
  "log4j" % "log4j" % "1.2.14"
)

//lazy val flinkVersion = "1.3.1"
lazy val flinkVersion = "1.2.0"
lazy val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion
)

initialize := {
  val _ = initialize.value
  // run the previous initialization
  val required = javaVersion
  val current = sys.props("java.specification.version")
  assert(current == required, s"Unsupported JDK: expected $required but actual is $current")
}

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies,
    libraryDependencies ++= flinkDependencies.map(_ % "compile"),
//    libraryDependencies ++= flinkDependencies.map(_ % "provided"),
    libraryDependencies ++= Seq("ml.dmlc" % "xgboost4j" % "0.7"),
    libraryDependencies ++= Seq("ml.dmlc" % "xgboost4j-flink" % "0.7"),
    libraryDependencies ++= Seq( "org.apache.flink" %% "flink-ml" % flinkVersion)
  )

lazy val commonSettings = Seq(
  organization := "hu.sztaki.ilab",
  version := "0.1.0",
  scalaVersion := "2.11.7",
  assemblyMergeStrategy in assembly := {
    case "log4j.properties" => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x => MergeStrategy.last
  },
  test in assembly := {}
)
