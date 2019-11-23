// give the user a nice default project!

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.8"
    )),
    name := "json_reader_fadeev",
    version := "0.0.1",

    sparkVersion := "2.3.0",
    sparkComponents := Seq(),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,

    coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
      //"org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
      "org.json4s" %% "json4s-jackson" % "3.6.7", //enable json4s
      //"org.apache.spark"  %% "spark-core" % "2.3.0" % "provided",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.json4s" %% "json4s-native" % "3.6.7",
      "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
      "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.10.0",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0",
      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % "test" 
    ),
/*
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-jackson" % "3.2.11",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.8.1" force(),

      "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",

      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % "test"
    ),
*/
    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },

   resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    pomIncludeRepository := { x => false },

    // publish settings
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )
