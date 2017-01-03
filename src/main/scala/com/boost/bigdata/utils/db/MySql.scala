package com.boost.bigdata.utils.db

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.boost.bigdata.utils.config.Configer
import com.mchange.v2.c3p0.ComboPooledDataSource

object MySql extends Configer {

  override val appname = "MySql"

  private val MYSQL_DRIVER: String = "com.mysql.jdbc.Driver"
  private val DATA_SOURCE_NAME: String = s"${appname}_connection_pool"
  private var pool_id: Int = 0

  setConfigFiles("mysql.conf")

  def createMysqlPool(): MysqlPool = {
    var pool: MysqlPool = new MysqlPool
    if (!pool.createInnerPool) {
      pool.closeInnerPool
      pool = null
    }
    pool
  }

  def closeMysqlPool(pool: MysqlPool) {
    if (pool != null) {
      pool.closeInnerPool
    }
  }

  def createConnection(server: String, port: Int, db: String, user: String, passwd: String, charset: String): Connection = {
    var conn: Connection = null
    try {
      Class.forName(MYSQL_DRIVER)
      DriverManager.setLoginTimeout(60)
      conn = DriverManager.getConnection(getConnectUrl(server, port, db, charset), user, passwd)
    }
    catch {
      case e: Exception => {
        log.warn(e.getMessage)
      }
    }
    conn
  }

  def createConnection: Connection = {
    var conn: Connection = null
    try {
      conn = createConnection(config.getString("mysql.server"), config.getInt("mysql.port"), config.getString("mysql.db"), config.getString("mysql.user"), config.getString("mysql.password"), config.getString("mysql.charset"))
    }
    catch {
      case e: Exception => {
        log.warn(e.getMessage, e)
      }
    }
    conn
  }

  def closeConnection(conn: Connection) {
    if (conn != null) {
      try {
        conn.close
      }
      catch {
        case e: Exception => {
        }
      }
    }
  }

  def closeStatement(stmt: Statement) {
    if (stmt != null) {
      try {
        stmt.close
      }
      catch {
        case e: Exception => {
        }
      }
    }
  }

  def closeResultSet(rs: ResultSet) {
    if (rs != null) {
      try {
        rs.close
      }
      catch {
        case e: Exception => {
        }
      }
    }
  }

  def closeMysqlSession(session: Session) {
    if (session != null) {
      session.disconnect
    }
  }

  def getConnectUrl(server: String, port: Int, db: String, charset: String): String = {
    s"jdbc:mysql://$server:$port/$db?socketTimeout=300000&characterEncoding=$charset"
  }

  object MysqlPool {
    def validatePool(pool: ComboPooledDataSource): Boolean = {
      var is_valid = false
      if (pool != null) {
        var conn: Connection = null
        try {
          conn = pool.getConnection
          if (conn != null) {
            is_valid = true
          }
        }
        catch {
          case e: Exception => {
            log.warn(e.getMessage)
          }
        } finally {
        }
      }
      is_valid
    }
  }

  class MysqlPool {
    private var pool: ComboPooledDataSource = null
    def createInnerPool: Boolean = {
      try {
        val server: String = config.getString("mysql.server")
        val port: Int = config.getInt("mysql.port")
        val db: String = config.getString("mysql.db")
        val user: String = config.getString("mysql.user")
        val passwd: String = config.getString("mysql.password")
        val charset: String = config.getString("mysql.charset") //, "utf8")
        val timeout: Int = config.getInt("mysql.login.timeout") //, 60)
        val min_pool_size: Int = config.getInt("min.pool.size") //, 10)
        val max_pool_size: Int = config.getInt("max.pool.size") //, 20)
        val init_pool_size: Int = config.getInt("init.pool.size") //, 10)
        val increment_num: Int = config.getInt("increment.num") //, 5)
        val max_idle_time: Int = config.getInt("max.idle.time") //, 300)
        pool = new ComboPooledDataSource(true)
        pool.setDataSourceName(DATA_SOURCE_NAME + "_" + pool_id)
        pool_id += 1
        pool.setJdbcUrl(getConnectUrl(server, port, db, charset))
        pool.setLoginTimeout(timeout)
        pool.setDriverClass(MYSQL_DRIVER)
        pool.setUser(user)
        pool.setPassword(passwd)
        pool.setMaxPoolSize(max_pool_size)
        pool.setMinPoolSize(min_pool_size)
        pool.setInitialPoolSize(init_pool_size)
        pool.setAcquireIncrement(increment_num)
        pool.setMaxIdleTime(max_idle_time)
        pool.setTestConnectionOnCheckout(true)
        pool.setAutoCommitOnClose(true)
        if (!MysqlPool.validatePool(pool)) {
          this.closeInnerPool
          pool = null
        }
      }
      catch {
        case e: Exception => {
          log.warn(e.getMessage)
          this.closeInnerPool
          pool = null
        }
      }
      pool != null
    }

    def getInnerPool: ComboPooledDataSource = {
      pool
    }

    def closeInnerPool {
      if (pool != null) {
        try {
          pool.close
        }
        catch {
          case e: Exception => {
            log.warn(e.getMessage)
          }
        }
      }
    }
  }

}
