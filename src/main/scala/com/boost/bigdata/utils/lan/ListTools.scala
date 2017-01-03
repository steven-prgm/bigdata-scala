package com.boost.bigdata.utils.lan


object ListTools {

  // example : mSort((x:Int,y,Int) => x < y)(List(1,3,5,7,2,4,6,8))

   /***
   *  you can also create a function object like this:
     *
   *  val IntSort = mSort((x:Int, y:Int) => x < y) _
   *  scala> IntSort:(List[Int]) => List[Int] = <function>
   *  val mixIntInts = List(2,6,9,8,3,4,1,7)
   *  scala> mixInts: List[Int] = List(2,6,9,8,3,4,1,7)
   *  IntSort(mixInts)
   *  res0: List(1,2,3,4,6,7,8,9)
     *
   ***/


  def mSort[T](less: (T, T) => Boolean)
              (src: List[T]): List[T] = {

    def merge(left: List[T], right: List[T]): List[T] = {
      (left, right) match {
        case (_, Nil) => left
        case (Nil, _) => right
        case (lh :: lt, rh :: rt) =>
          if (less(lh, rh)) lh :: merge(lt, right)
          else rh :: merge(left, rt)
      }
    }

    val n = src.length / 2
    if (n == 0) src
    else {
      val (ls, rs) = src.splitAt(n)
      merge(mSort(less)(ls), mSort(less)(rs))
    }
  }

}
