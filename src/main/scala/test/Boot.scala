package test

import java.io.File
import com.boost.bigdata.utils.file.FileTools
import com.boost.bigdata.utils.log.LogSupport
import common.BoostConfiger
import java.sql.{DriverManager, Connection, ResultSet}


object Boot extends LogSupport{

  BoostConfiger


  val user = "root"
  val password = "root123"
  val host = "192.168.24.131"
  val database = "db_spacewalker"
  val conn_str = "jdbc:mysql://" + host + ":3306/" + database + "?user=" + user + "&password=" + password
  //println("connect string => " + conn_str)

  def main(args: Array[String]) {



  }



  def pathTest = {

    val path = "/dir1/dir2/dir3/dir4/a.txt"
    val path1 = "/test"
    val files = path.split("/").filter(!_.isEmpty)

    log.info("files.length => " + files.length)

    var curPath = ""
    for (i <- 0 until files.length) {
      curPath = files.dropRight(i).mkString("/","/","")
      println("curPath: => " + curPath)
    }

    val num = files.length -1

    for (i <- 0 until num) {
      val curPath = files.dropRight(num - i).mkString("/","/","")
      println("curPath num: => " + curPath)
    }


    val mkStr = files.mkString("/","/","")
    println(mkStr)

  }


  def fileToolsTest = {

    val file = new File( s"""F:/scala/test/folder""")
    val res = FileTools.CreateFolderWithParent(file)

    println("Folder test => " + res)
  }

  def configTest = {

    /*val host = AcGameConfiger.mysql.host
    val port = AcGameConfiger.mysql.port
    val user = AcGameConfiger.mysql.user
    val password = AcGameConfiger.mysql.password

    val test = AcGameConfiger.test

    //log.info("mysql: host => " + host)
    println("mysql: host => " + host)
    println("mysql: port => " + port)
    println("mysql: user => " + user)
    println("mysql: password => " + password)
    println("test: => " + test)*/

  }

  def mysqlTest() = {
    //classOf[com.mysql.jdbc.Driver]
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = DriverManager.getConnection(conn_str)
    println("hello")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs: ResultSet = statement.executeQuery("SHOW TABLES")

      // Iterate Over ResultSet
      while (rs.next) {
        println(rs.getRow())
      }
    }
    catch {
      case _: Exception => println("===>")
    }
    finally {
      conn.close
    }
  }

}


