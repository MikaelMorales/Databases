import AssemblyKeys._ // put this at the top of the file

seq(assemblySettings: _*)

name := "CS422-Project2"

version := "0.1.0"

scalaVersion := "2.11.7"

jarName in assembly := "cs422_p2.jar"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
                            "org.slf4j" % "slf4j-simple" % "1.7.5")

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.3.1"
)

libraryDependencies += "com.twitter" %% "algebird-core" % "0.8.0"
libraryDependencies += "com.twitter" %% "algebird-spark" % "0.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.0.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1"
)
                            
libraryDependencies += "com.typesafe" % "config" % "1.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

// Avoid generating eclipse source entries for the java directories
(unmanagedSourceDirectories in Compile) <<= (scalaSource in Compile)(Seq(_))

(unmanagedSourceDirectories in Test) <<= (scalaSource in Test)(Seq(_))

mergeStrategy in assembly <<= (mergeStrategy in assembly) ((old) => {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first // Changed deduplicate to first
    }
  case PathList(_*) => MergeStrategy.first // added this line
})
