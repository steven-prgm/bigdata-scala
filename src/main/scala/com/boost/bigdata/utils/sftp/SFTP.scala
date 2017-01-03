package com.boost.bigdata.utils.sftp

import java.io.{FileOutputStream, File, InputStream}

import com.boost.bigdata.utils.ftp.FtpManager
import com.boost.bigdata.utils.log.LogSupport
import com.jcraft.jsch.ChannelSftp


class SFTP(server: String, port: String, user: String, password: String) extends
FtpManager(server: String, port: String, user: String, password: String) with LogSupport {

  def usingSftp(op: ChannelSftp => Unit) = {
    try {
      val channel: SFTPChannel = getChannel()
      val channelSftp: ChannelSftp = getChannelSFTP(channel)
      op(channelSftp)
      channel.closeChannel()
    } catch {
      case e: Exception => log.warn("SFTPManager => " + e.printStackTrace)
    }
  }

  protected def getChannel(): SFTPChannel = {
    new SFTPChannel()
  }

  protected def getChannelSFTP(channel: SFTPChannel): ChannelSftp = {
    val sftpDetails: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    // 设置主机ip，端口，用户名，密码
    sftpDetails.put(SFTPConstants.SFTP_REQ_HOST, server)
    sftpDetails.put(SFTPConstants.SFTP_REQ_USERNAME, user)
    sftpDetails.put(SFTPConstants.SFTP_REQ_PASSWORD, password)
    sftpDetails.put(SFTPConstants.SFTP_REQ_PORT, port)
    channel.getChannel(sftpDetails, 60000)
  }

  // test failed
  override def listFiles(parentPath: String): Array[String] = {
    var array = Array[String]()
    usingSftp {
      channelSftp =>
        val result = channelSftp.ls(parentPath).toArray
        val files = result.filterNot(x => x.toString.startsWith("d"))
        val names = files.toList.map(x => x.toString.split(" ").last)
        channelSftp.quit()
        array = new Array[String](names.size)
        names.copyToArray(array, 0)
    }
    array
  }

  override def listDirectories(parentPath: String): Array[String] = {
    var array = Array[String]()
    usingSftp {
      ChannelSftp =>
        val result = ChannelSftp.ls(parentPath).toArray
        ChannelSftp.quit()
        val directory = result.filter(x => x.toString.startsWith("d"))
        val dirs = directory.toList.map(x => x.toString.split(" ").last)
        val folders = dirs.filterNot(x => x == "." || x == "..")
        array = new Array[String](folders.size)
        folders.copyToArray(array, 0)
    }
    array
  }

  //upload file ok
  override def upload(local: String, remote: String): Boolean = {
    var res = false
    usingSftp {
      channelSftp =>
        MakeRemoteDirectory(channelSftp, remote)
        channelSftp.put(local, remote, ChannelSftp.OVERWRITE)
        channelSftp.quit()
        res = true
    }
    res
  }

  override def upload(localStream: InputStream, remote: String): Unit = {
    usingSftp {
      channelSFTP =>
        val remotepath = MakeRemoteDirectory(channelSFTP, remote)
        val filename = remote.substring(remote.lastIndexOf("/"))
        channelSFTP.put(localStream, remote, ChannelSftp.OVERWRITE)
        channelSFTP.quit()
    }
  }

  //download file ok
  override def download(src: String, dst: String, timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Unit = {
    usingSftp {
      channelSftp =>
        if (fileIsExists(channelSftp, src)) {
          var index = src.lastIndexOf('/')
          if (index == -1) index = src.lastIndexOf('\\')
          val fileName = src.substring(index + 1, src.length)
          val localFile = new File(dst + "/" + fileName)
          if (!localFile.getParentFile.exists)
            localFile.getParentFile.mkdirs
          val outputStream = new FileOutputStream(localFile)
          channelSftp.get(src, outputStream)
          outputStream.close()
        }
        channelSftp.quit()
    }
  }

  override def downloadFiles(src: List[String], dst: String, timeout: Int = FtpManager.FTP_DATA_TIMEOUT_DEFAULT): Unit = {
    usingSftp {
      channelSftp =>
        src.foreach(e => {
          if (fileIsExists(channelSftp, e)) {
            var index = e.lastIndexOf('/')
            if (index == -1) index = e.lastIndexOf('\\')
            val fileName = e.substring(index + 1, e.length)
            val localFile = new File(dst + "/" + fileName)
            if (!localFile.getParentFile.exists)
              localFile.getParentFile.mkdirs
            val is = new FileOutputStream(localFile)
            channelSftp.get(e, is)
            is.close()
          }
        })
        channelSftp.quit()
    }
  }

  override def deleteDirectory(remote: String): Int = {
    var isDeleteDirectorySuccess: Int = -1
    usingSftp {
      channelSftp =>
        val remoteWithoutPoint: String = remote.take(1) match {
          case "." => remote.drop(1)
          case _ => remote
        }
        isDeleteDirectorySuccess = deleteDirectory(channelSftp, remoteWithoutPoint)
        channelSftp.quit()
    }
    isDeleteDirectorySuccess
  }

  //delete directory test ok
  private def deleteDirectory(ChannelSFTP: ChannelSftp, remoteWithoutPoint: String): Int = {
    try {
      val result = ChannelSFTP.ls(remoteWithoutPoint).toArray
      val (dirList, fileList) = result.partition(x => x.toString.startsWith("d"))
      val foldersAll = dirList.toList.map(x => x.toString.split(" ").last)
      val folders = foldersAll.filterNot(x => x == "." || x == "..")
      val fileNames = fileList.toList.map(x => x.toString.split(" ").last)
      if (!fileNames.isEmpty)
        fileNames.foreach(f => ChannelSFTP.rm(s"/$remoteWithoutPoint/$f"))
      if (folders.isEmpty)
        ChannelSFTP.rmdir(s"/$remoteWithoutPoint")
      else {
        folders.foreach(f => {
          val filePath = s"$remoteWithoutPoint/$f"
          deleteDirectory(ChannelSFTP, filePath)
        })
        ChannelSFTP.rmdir(remoteWithoutPoint)
      }
      0
    }
    catch {
      case e: Exception =>
        //       LOG.debug("delete Directory exception!")
        -1
    }
  }

  //delete foldor ok
  def deletefolder(folder: String): Unit = {
    usingSftp {
      channelSftp =>
        channelSftp.rmdir(folder)
        channelSftp.quit()
    }
  }

  //delete file ok
  override def delete(pathname: String): Unit = {
    usingSftp {
      channelSftp =>
        if (fileIsExists(channelSftp, pathname)) {
          channelSftp.rm(pathname)
        }
        channelSftp.quit()
    }
  }

  override def downloadByExt(srcDir: String, baseDstDir: String, ext: String): Unit = {
    usingSftp {
      channelSftp =>
        downloadByExt(channelSftp, srcDir, baseDstDir, ext)
        channelSftp.quit()
    }
  }

  private def downloadByExt(channelSftp: ChannelSftp, srcDir: String, baseDstDir: String, ext: String): Unit = {
    try {
      val result = channelSftp.ls(srcDir).toArray
      val direction = result.partition(x => x.toString.startsWith("d"))
      val foldersall = direction._1.toList.map(x => x.toString.split(" ").last)
      val folders = foldersall.filterNot(x => x == "." || x == "..")
      val filenames = direction._2.toList.map(x => x.toString.split(" ").last)

      val file = new File(baseDstDir + "/test")
      if (!file.getParentFile.exists)
        file.getParentFile.mkdirs
      filenames.foreach(x => if (x.endsWith(ext)) channelSftp.get(s"$srcDir/$x", s"$baseDstDir/$x"))

      folders.foreach(x => downloadByExt(channelSftp, s"$srcDir/$x", s"$baseDstDir/$x", ext))
    }
    catch {
      case _: Exception =>
      //        LOG.debug("downloadByExt file exception!")
    }
  }

  /**
   * 递归遍历和下载指定目录下面指定后缀名的文件
   * relativePath 相对路径，用来记录在ftp内当前目录的相对路径。在调用时该参数设置为""，后面递归调用时会设置为非空值。
   * srcDir 需要遍历的目录
   * baseDstDir 目标目录，必须是绝对路径
   * ext 文件的扩展名
   */
  private def MakeRemoteDirectory(channelSftp: ChannelSftp, remote: String): String = {
    try {
      def remotepathVerified(path: String): String = path.take(1) match {
        case "." => remotepathVerified(path.drop(1))
        case "/" => path.drop(1)
        case _ => path
      }
      var checkedRemotePath = remotepathVerified(remote)
      val directories = checkedRemotePath.split('/')
      val folder = directories.head.toString
      val result = channelSftp.ls(s"/$folder").toArray
      val direction = result.filter(x => x.toString.startsWith("d"))
      if (directories.length > 2)
        if (!direction.toList.map(x => x.toString.split(" ").last).contains(directories(1).toString)) {
          channelSftp.cd(folder)
          channelSftp.mkdir(directories(1))
          checkedRemotePath = directories(1)
        }
      checkedRemotePath
    }
    catch {
      case _: Exception =>
        //       LOG.debug("downloadByExt file exception!")
        ""
    }
  }

  private def fileIsExists(channelSftp: ChannelSftp, file: String): Boolean = {
    try {
      var index = file.lastIndexOf('/')
      if (index == -1) index = file.lastIndexOf('\\')
      val parentPath = file.substring(0, index)
      val filename = file.substring(index + 1, file.length)
      val result = channelSftp.ls(parentPath).toArray
      val files = result.filterNot(x => x.toString.startsWith("d"))
      val names = files.toList.map(x => x.toString.split(" ").last)
      if (names.contains(filename))
        true
      else
        false
    }
    catch {
      case _: Exception =>
        //       LOG.debug("check file exists exception!")
        false
    }
  }

}

object SFTP {
  def apply(conInfo: SFtpConnectionInfo): SFTP =
    new SFTP(conInfo.ip, conInfo.port, conInfo.user, conInfo.password)
}

case class SFtpConnectionInfo(user: String,
                              password: String,
                              ip: String,
                              port: String)
