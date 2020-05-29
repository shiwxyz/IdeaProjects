package com.shiwxyz.bigdata.utils

object ESUtils {

  def createESOption(options: Map[String, String]): Map[String, String] = {
    if (null == options || options.size <= 0) {
      Map(
        "es.index.auto.create" -> "",
        "es.nodes" -> "",
        "es.port" -> "",
        "es.http.timeout" -> "1800s",
        "es.batch.write.retry.count" -> "-1",
        "es.batch.size.bytes" -> "20mb",
        "es.batch.write.retry.wait" -> "900s",
        "es.batch.size.entries" -> "5000"
      )
    } else {
      Map(
        "es.index.auto.create" -> "",
        "es.nodes" -> "",
        "es.port" -> "",
        "es.http.timeout" -> "1800s",
        "es.batch.write.retry.count" -> "-1",
        "es.batch.size.bytes" -> "20mb",
        "es.batch.write.retry.wait" -> "900s",
        "es.batch.size.entries" -> "5000"
      ) ++ options
    }
  }
}
