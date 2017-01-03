package com.boost.bigdata.utils.tools

import java.security.MessageDigest

object MD5 {

  def md5Hash(text: String): String = {
    MessageDigest.getInstance("MD5").digest(text.getBytes).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_ + _}
  }

}
