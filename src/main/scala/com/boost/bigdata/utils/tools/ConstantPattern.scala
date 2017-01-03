package com.boost.bigdata.utils.tools


object ConstantPattern {

  def main(args: Array[String]) {
    println(patternShow(5))
    println(patternShow(true))
    println(patternShow(List()))
  }

  def patternShow(x: Any) = x match {
    case 5 => "五"
    case true => "真"
    case "test" => "字符串"
    case null => "null值"
    case Nil => "空列表"
    case _ => "其他常量"
  }

}

