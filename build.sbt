import Dependencies._

val flinkVersion = "1.4.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "co.sjwiesman",
      scalaVersion := "2.11.12",
      version      := "1.0"
    )),
    name := "ff-sf-2018",
    libraryDependencies ++= Seq (
      "org.apache.flink" %% "flink-scala"           % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-java"  % flinkVersion % "provided",
      "org.apache.flink" %% "flink-streaming-java"  % flinkVersion   % Test classifier "tests",
      "org.apache.flink" %% "flink-streaming-contrib" % flinkVersion % Test,
      "org.apache.flink" %% "flink-test-utils"       % flinkVersion  % Test,
      "org.apache.flink" %% "flink-tests"            % flinkVersion  % Test classifier "tests",
      "org.apache.flink" %% "flink-runtime"          % flinkVersion  % Test classifier "tests",
      "org.apache.flink" %% "flink-runtime"          % flinkVersion  % Test
    ),
    libraryDependencies ++= Seq(scalaTest, mockito, junit).map(_ % Test),
    testGrouping in Test := TestSort.sorting.value,
    parallelExecution in Test := false,
    logBuffered in Test := false
  )
