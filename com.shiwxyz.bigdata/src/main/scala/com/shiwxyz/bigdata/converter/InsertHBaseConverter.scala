package com.shiwxyz.bigdata.converter

import com.shiwxyz.bigdata.utils.HBaseConverter
import org.apache.commons.net.ntp.TimeStamp
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.apache.spark.sql.Row


object InsertHBaseConverter {

  class PutOps(val p: Put) extends AnyVal {
    /**
     *
     * @param cf    列簇
     * @param field 字段名
     * @param value 字段值
     * @return
     */
    def addValue(cf: String, field: String, value: Array[Byte]): Put =
      p.addColumn(toBytes(cf), toBytes(field), value)

    /**
     * 不同类型的字段转换为string类型
     *
     * @param cf
     * @param field
     * @param valueOpt
     * @return
     */
    def addOptionValue(cf: String, field: String, valueOpt: Any): Put =
      valueOpt match {
        case v: String =>
          addValue(cf, field, toBytes(v))
        case v: Int =>
          addValue(cf, field, toBytes(v.toString))
        case v: Long =>
          addValue(cf, field, toBytes(v.toString))
        case v: Double =>
          addValue(cf, field, toBytes(v.toString))
        case v: TimeStamp =>
          addValue(cf, field, toBytes(v.toString))
        case _ => p
      }
  }

  implicit def PutOps(p: Put): PutOps = new PutOps(p)

  implicit object SB extends HBaseConverter[Row] {
    override def toPut(t: Row, cf: String): Put = {
      val name = t.getAs[String]("name")
      val age = t.getAs[Int]("age")
      val sex = t.getAs[String]("sex")
      val rowkey = t.getAs[String]("rowkey")

      val p = new Put(rowkey.getBytes)
      p.addOptionValue(cf, "name", name)
        .addOptionValue(cf, "age", age)
        .addOptionValue(cf, "sex", sex)
      p
    }
  }
}
