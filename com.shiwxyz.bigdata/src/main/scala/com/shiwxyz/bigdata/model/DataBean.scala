package com.shiwxyz.bigdata.model

import org.apache.spark.sql.Row

class DataBean(
                val id_mln_emp: String,
                val user_id: String,
                val emp_name: String,
                val sex: String,
                val age: Double
              ) extends Product with Serializable {
  //模式匹配
  override def productElement(n: Int): Any = n match {
    case 0 => id_mln_emp
    case 1 => user_id
    case 2 => emp_name
    case 3 => sex
    case 4 => age
    case _ =>
  }

  //返回传参的个数
  override def productArity: Int = 5

  //自定义比较，是否包含这个数
  override def canEqual(that: Any): Boolean = that.isInstanceOf[DataBean]

}

object DataBean {
  def buildFormat(line:Row):DataBean = {
    new DataBean(line.getAs[String](""),line.getAs[String](""),line.getAs[String](""),
      line.getAs[String](""),line.getAs[Double](""))
  }
}



