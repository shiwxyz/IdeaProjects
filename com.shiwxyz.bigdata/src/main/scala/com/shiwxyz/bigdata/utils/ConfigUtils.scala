package com.shiwxyz.bigdata.utils

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.util.Bytes

object ConfigUtils extends LazyLogging {

  var env = ""
  lazy val config: Config = {
    env = sys.env.getOrElse("env", "")
    print("env", env)
    if (env == "") ConfigFactory.load()
    else ConfigFactory.load(s"application-${env}")
  }

  //HBase Config
  lazy val HBASE_ZK_QUORUM_PATH = "hbase.zookeeper.quorum"
  lazy val HBASE_ZK_PORT_PATH = "hbase.zookeeper.property.clientPort"
  lazy val HBASE_ZK_PARENT_PATH = "hbase.zookeeper.znode.parent"
  lazy val HBASE_ZK_QUORUM = config.getString(HBASE_ZK_QUORUM_PATH)
  lazy val HBASE_ZK_PORT = config.getString(HBASE_ZK_PORT_PATH)
  lazy val HBASE_ZK_PARENT = config.getString(HBASE_ZK_PARENT_PATH)

  val SPLIT_KEYS: Array[Array[Byte]] = Array(
    Bytes.toBytes("1"),
    Bytes.toBytes("2"),
    Bytes.toBytes("3"),
    Bytes.toBytes("4"),
    Bytes.toBytes("5"),
    Bytes.toBytes("6"),
    Bytes.toBytes("7"),
    Bytes.toBytes("8"),
    Bytes.toBytes("9"),
    Bytes.toBytes("0")
  )

  //Hive Config


}
