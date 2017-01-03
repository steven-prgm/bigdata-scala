package com.boost.bigdata.utils.tools

object test{

  val KEY_TAG = "###"

  def makeUserDimensionKeys(uid: String, productId: String, dimensions: Array[String]): List[String] = {
    //configId: String, desc: String, contentType: String, language: String
    //    val dimensions = List("config_id", "scenario", "content_type", "language")

    val detailKey = dimensions.mkString(KEY_TAG) //ckonfig_id###scenario###content_type###language

    //生成维度组合Keys,并去重
    val result = compress(genDimensionKeys(detailKey,dimensions).sorted)

    //加上原有的最详细的维度key => [config_id###scenario###content_type###language]
    val dimensionKeys = compress(result) ::: List(detailKey)

    //uid, productId 与 维度key 拼接
    dimensionKeys.map(dimensionKey => s"$uid$KEY_TAG$productId$KEY_TAG$dimensionKey")
  }

  def genDimensionKeys(dimKey: String, dimArr: Array[String]): List[String] = {

    val dimSubArr = dimArr.filter(dimKey.contains(_))

    val keys = for (dim <- dimSubArr) yield {
      dimKey.replace(dim, "all")
    }

    val subKeys: List[String] = keys.map(genDimensionKeys(_, dimSubArr)).flatten.toList
    subKeys ::: keys.toList
  }

  // Tail recursive. 对相邻的元素去重,需要提前排序
  def compress[A](ls: List[A]): List[A] = {
    def compressR(result: List[A], curList: List[A]): List[A] = curList match {
      case h :: tail => compressR(h :: result, tail.dropWhile(_ == h))
      case Nil => result.reverse
    }
    compressR(Nil, ls)
  }

}
