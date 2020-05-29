package com.shiwxyz.bigdata.utils

import com.typesafe.config.ConfigFactory
import slick.driver.{JdbcProfile, PostgresDriver}

trait DBComponent {
  val driver: JdbcProfile

  import driver.api._

  def db: Database
}

trait PostgresComponent extends DBComponent {
  val driver = PostgresDriver

  import driver.api._

  def db: Database = PostgresDB.connectionPool
}

private[jdbc] object PostgresDB {

  import slick.driver.PostgresDriver.api._

  lazy val connectionPool = {
    lazy val env = sys.env.get("env").orElse(sys.props.get("env")).getOrElse("")

    if (env == "") {
      Database.forConfig("slickPg")
    } else {
      lazy val configEnv = ConfigFactory.load("")
      Database.forConfig("slickPg", configEnv)
    }
  }

}