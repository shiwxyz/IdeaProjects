package com.shiwxyz.bigdata.utils

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

trait HBaseConverter[T] extends Serializable {

  def toWriteablePut(t:T,cf:String):(ImmutableBytesWritable,Put) = (new ImmutableBytesWritable(),toPut(t,cf))

  def toPut(t:T,cf:String):Put
}
