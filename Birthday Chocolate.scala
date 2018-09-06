import java.io._
import java.math._
import java.security._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._

object Solution {

    // Complete the solve function below.
    def solve(s: Array[Int], d: Int, m: Int): Int = {
        var res: Int = 0

        if(s.length <= m){
           if(s.reduce(_ + _) == d ) res += 1 
        }
        else 
            for(i <- 0 until s.length - (m - 1))){
                var sum = 0
                for( j <- 0 until m){
                    sum += s(i + j)
                }
                if(sum == d) res += 1
            }
        res
    }

    def main(args: Array[String]) {
        val stdin = scala.io.StdIn

        val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val n = stdin.readLine.trim.toInt

        val s = stdin.readLine.split(" ").map(_.trim.toInt)
        val dm = stdin.readLine.split(" ")

        val d = dm(0).trim.toInt

        val m = dm(1).trim.toInt

        val result = solve(s, d, m)

        printWriter.println(result)

        printWriter.close()
    }
}
