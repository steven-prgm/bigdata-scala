import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

name := "boost-utils"

version := "1.0.0"

scalaVersion := "2.11.5"

resolvers += "typesafe Repository" at "http://repo.tysafe.com/typesafe/releases/"

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.13",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.poi" % "poi" % "3.10.1",
  "org.apache.poi" % "poi-ooxml" % "3.10.1",
  "org.apache.poi" % "poi-ooxml-schemas" % "3.10.1",
  "org.scala-lang" % "scala-library" % "2.11.5",
  "com.typesafe" % "config" % "1.2.1",
  "net.sf.opencsv" % "opencsv" % "2.3",
  "net.sf.jpam" % "jpam" % "1.1",
  "net.sf.py4j" % "py4j" % "0.8.2.1",
  "org.apache.ant" % "ant" % "1.9.1",
  "org.apache.ant" % "ant-launcher" % "1.9.1",
  "com.jcraft" % "jsch" % "0.1.42",
  "commons-net" % "commons-net" % "3.3",
  "org.apache.hadoop" % "hadoop-common" % "2.3.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.3.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "ch.qos.logback" % "logback-core" % "1.1.2",
  "javax.mail" % "mail" % "1.4.7",
  "mysql" % "mysql-connector-java" % "5.1.38",
  "org.apache.zookeeper" % "zookeeper" % "3.4.6",
  "org.apache.spark" % "spark-core_2.10" % "1.6.2",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.2",
  "redis.clients" % "jedis" % "2.7.3",
  "com.alibaba" % "fastjson" % "1.2.7",
  "c3p0" % "c3p0" % "0.9.1.2"
)

// "org.slf4j" % "slf4j-api" % "1.7.10" % "runtime",
// "org.slf4j" % "slf4j-log4j12" % "1.7.13",
// "org.slf4j" % "jcl-over-slf4j" % "1.7.10",
// "org.slf4j" % "jul-to-slf4j" % "1.7.10",

// "dom4j" % "dom4j" % "1.6.1"
// "c3p0" % "c3p0" % "0.9.1.2"
// "junit" % "junit" % "4.12"

// "org.slf4j" % "slf4j-log4j12" % "1.7.5",
// "org.slf4j" % "slf4j-api" % "1.7.7",

//------------------------------------------------------------
// ���ô�����
//------------------------------------------------------------
//use sbt-assembly settings

assemblySettings

jarName in assembly := "boost-utils_1.0.0.jar"

test in assembly := {}

//mainClass in assembly := Some("test.Boot")

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case PathList(ps@_*) if ps.last endsWith ".thrift" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".default" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".RSA" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".class" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".xsd" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".dtd" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".txt" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".jar" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".providers" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith "mailcap" => MergeStrategy.first
    case x => old(x)
  }
}

