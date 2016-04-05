name := "Kitsch"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka"     %% "akka-actor"         % "2.3.14",
  "com.typesafe.akka"     %% "akka-remote"        % "2.3.14",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "org.scalanlp"          %% "breeze"             % "0.11.2",
  "org.scalanlp"          %% "breeze-natives"     % "0.11.2",
  "org.scalanlp"          %% "breeze-viz"         % "0.11.2",
  "com.opencsv" % "opencsv" % "3.6",
  "com.google.guava"      %  "guava"              % "19.0",
  "com.google.code.findbugs" % "jsr305" % "2.0.3"
)

resolvers ++= Seq(
  // other resolvers here
  // if you want to use snapshot builds (currently 0.12-SNAPSHOT), use this.
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

licenses += ("Apache-2.0", url("https://opensource.org/licenses/Apache-2.0"))
