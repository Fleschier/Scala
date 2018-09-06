import java.io._
import java.math._
import java.security._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._

import scala.collection.mutable._

object Solution {

    // Complete the breakingRecords function below.
    def breakingRecords(scores: Array[Int]): Array[Int] = {
        val res = ArrayBuffer(0,0)
        var max,min = scores(0)

        scores.foreach{x =>
            if(x > max) {
                res(0) += 1
                max = x
            }
            if(x < min) {
                res(1) += 1
                min = x
            }
        }
        res.toArray
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val n = stdin.readLine.trim.toInt

        val scores = stdin.readLine.split(" ").map(_.trim.toInt)
        val result = breakingRecords(scores)

        printWriter.println(result.mkString(" "))

        printWriter.close()
    }
}


//other's solution
import scala.io.StdIn
    
object Solution {

    def main(args: Array[String]) {
        val n = StdIn.readInt()
        val scores = StdIn.readLine().split(" ").map(_.toInt)
        val record = scores.tail.foldLeft((scores.head, 0, scores.head, 0)) { (r, score) =>
            if (score > r._1) (score, r._2 + 1, r._3, r._4)
            else if (score < r._3) (r._1, r._2, score, r._4 + 1)
            else r
        }
        printf("%d %d\n", record._2, record._4)
    }
}