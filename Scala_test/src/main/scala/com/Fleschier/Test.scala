package com.Fleschier

import scala.collection.mutable.ListBuffer

object Test {
  def main(args: Array[String])={

    test_func_001()


  }

  def test_func_001(): Unit ={
    //the usage of listbuffer
    //ListBuffer可以直接声明为val 类型
    val buf = new ListBuffer[Int]()
    buf.append(2)
    buf.append(3)
    //buf.foreach(println)
    val a = buf.toList  //将ListBuffer转化为List
    //a.foreach(println)
    a.foreach{x =>
      for(i <- 0 until 3)
        println(i)
    }
    a.foreach{x=>
      println(x)
    }
  }
}
