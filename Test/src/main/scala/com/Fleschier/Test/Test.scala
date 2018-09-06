package com.Fleschier.Test

import scala.collection.mutable.ListBuffer
import java.io._

object Test {


    def myFilter(k: Int, arr:List[Int]): List[Int] = {
      arr.filter(x => x < k)
    }

    def main(args: Array[String]): Unit = {
      val br = new BufferedReader(new InputStreamReader(System.in))
      val x = br.readLine().trim.toInt
      val arr = ListBuffer[Int]()

      var flag = true
      while(flag){
        val s = br.readLine()
        if(s != "END"){
            arr.append(s.toInt)
        }
        else flag = false
      }

      println(myFilter(x,arr.toList))

    }


}
