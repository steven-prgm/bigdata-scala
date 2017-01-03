package common

import com.boost.bigdata.utils.config.Configer


object BoostConfiger extends Configer{

  def appname = "acgame"

  setConfigFiles("acgame.conf", "test.conf")

  val mysql = MysqlParam(config.getString("mysql.host"),
                          config.getString("mysql.port"),
                          config.getString("mysql.user"),
                          config.getString("mysql.password"))

  val test = config.getString("test.host")
}


