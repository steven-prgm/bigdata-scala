package com.boost.bigdata.utils.db

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import com.boost.bigdata.utils.log.LogSupport

class Session(connection: Connection) extends LogSupport{

  private var conn: Connection = connection
  private var stmt: Statement = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)

  def executeQuery(sql: String): ResultSet = {
    var rs: ResultSet = null
    try {
      if (stmt != null) {
        rs = stmt.executeQuery(sql)
      }
    }
    catch {
      case e: SQLException => {
        log.warn(e.getMessage)
      }
    }
    rs
  }

  def executeUpdate(sql: String): Boolean = {
    var ret: Boolean = false
    try {
      if (stmt != null) {
        stmt.executeUpdate(sql)
        ret = true
      }
    }
    catch {
      case e: SQLException => {
        log.warn(e.getMessage)
      }
    }
    ret
  }

  def disconnect {
    MySql.closeStatement(stmt)
    stmt = null
    MySql.closeConnection(conn)
    conn = null
  }
}

object Session{
  def apply(con: Connection) = new Session(con)
}

