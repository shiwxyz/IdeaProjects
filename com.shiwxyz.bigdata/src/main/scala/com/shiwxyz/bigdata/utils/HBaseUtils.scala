package com.shiwxyz.bigdata.utils

import com.shiwxyz.bigdata.utils.ConfigUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import com.shiwxyz.bigdata.constant.Constants._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.mapreduce.{Job,MRJobConfig, OutputFormat}

object HBaseUtils extends LazyLogging {

  def createConf(zkQuorum: String = HBASE_ZK_QUORUM, zkPort: String = HBASE_ZK_PORT): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(HBASE_ZK_QUORUM_PATH, zkQuorum)
    conf.set(HBASE_ZK_PORT_PATH, zkPort)
    conf.set(HBASE_ZK_PARENT_PATH, HBASE_ZK_PARENT)
    conf
  }

  /**
   *
   * @param tableName
   * @param columnFamilys
   */
  def createTableWithNoSplitKey(tableName: String, columnFamilys: String*): Unit = {
    val conf = createConf()
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val tn = TableName.valueOf(tableName)
    val tableDesc = new HTableDescriptor(tn)
    columnFamilys.foreach(f => {
      tableDesc.addFamily(new HColumnDescriptor((f.getBytes())))
    })
    if (!admin.tableExists(tn)) {
      admin.createTable(tableDesc)
    }
    admin.close()
  }

  /**
   *
   * @param tableName
   * @param regionKeys
   * @param columnFamilys
   */
  def createTableWithSplitKey(tableName: String, regionKeys: Array[Array[Byte]], columnFamilys: String*): Unit = {
    val conf = createConf()
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val tn = TableName.valueOf(tableName)
    val tableDesc = new HTableDescriptor(tn)
    columnFamilys.foreach(f => {
      tableDesc.addFamily(new HColumnDescriptor((f.getBytes())))
    })
    if (!admin.tableExists(tn)) {
      admin.createTable(tableDesc, regionKeys)
    }
    admin.close()
  }

  /**
   *
   * @param tableName
   * @param rdd
   * @param tmpDir
   */
  def bulkLoad(tableName: String, rdd: RDD[(ImmutableBytesWritable, KeyValue)], tmpDir: String = HBASE_TMP_DIR) = {
    val conf = createConf()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, HDFS_TMP_PATH)
    conf.addResource("hdfs-site.xml")

    val load = new LoadIncrementalHFiles(conf)

    val path = new Path(tmpDir)
    val fs = FileSystem.get(conf)
    if (fs.exists(path)) {
      fs.delete(path, true)
      logger.info(s"delete ${tmpDir} success")
    }

    rdd.saveAsNewAPIHadoopFile(tmpDir,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf
    )

    load.doBulkLoad(new Path(tmpDir), new HTable(conf, tableName))
  }

  /**
   *
   * @param rdd
   * @param tableName
   * @param cf
   * @param converter
   * @tparam T
   * @return
   */
  def rddWriteToHBase[T](rdd: RDD[T], tableName: String, cf: String)(implicit converter: HBaseConverter[T]) = {
    val conf = createConf()
    conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
      classOf[TableOutputFormat[_]], classOf[OutputFormat[_, _]]
    )
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    ignoreEmptyRDDHBase(rdd){(r)=>
      r.map(converter.toWriteablePut(_,cf)).saveAsNewAPIHadoopDataset(conf)
    }
  }

  /**
   *
   * @param rdd
   * @param action
   * @tparam T
   * @return
   */
  def ignoreEmptyRDDHBase[T](rdd: RDD[T])(action: (RDD[T]) => Unit): Boolean = {
    rdd.cache()
    if (rdd.isEmpty()) {
      print("RDD is Empty,no data save to hbase")
      rdd.unpersist(false)
      false
    } else {
      action(rdd)
      rdd.unpersist(false)
      true
    }
  }
}
