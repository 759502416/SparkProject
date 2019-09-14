package com.whx.grammer

/**
  * This Object is used to check the Scala grammer.
  * we can learn why wo choose this grammer rather than usr other grammer in our work
  */
object ScalaGrammerTest {
  def main(args: Array[String]) = {
    val array: Array[Array[String]] = Array(Array("1","2"),Array("1","2"),Array("1","2"),Array("1","2"))
    // the flatMap
    val tuples: Array[(String, Int)] = array.flatMap(x=>x.map(y=>(y,1)))
    tuples.foreach(println)

    println("----------------------------------------")
    // the Map
    val tuples2: Array[Array[(String, Int)]] = array.map(x=>x.map(y=>(y,1)))
    tuples2.foreach(x=>x.foreach(println))

  }
}
