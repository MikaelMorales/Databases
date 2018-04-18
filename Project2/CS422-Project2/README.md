CS422 - Project 2
====================

Import the project in Eclipse using the following command
> sbt eclipse

Build from command line
> sbt package

In order to execute your program in eclipse make the following change in Main.scala:23:

val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")

Otherwise take the jar file that is created after running `sbt package`.
The jar is located under target/scala-2.11/

