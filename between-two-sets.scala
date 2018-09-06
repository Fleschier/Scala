import java.io.PrintWriter

object Solution {

    /*
     * Complete the getTotalX function below.
     */
    def getTotalX(a: Array[Int], b: Array[Int]): Int = {
        /*
         * Write your code here.
         */
       val min = b.min
       val max = a.max
       var res = 0

       for(i <- max to min){  //for循环的迭代器前一个必须比后一个要小，否则会返回一个Unit类型的值
        var flag: Boolean = true
        a.foreach{x => 
            if(i % x != 0)
                flag = false
        }
        b.foreach{ y=>
            if(y % i != 0)
                flag = false
        }
        if(flag)
            res += 1
       }
       res

    }

    def main(args: Array[String]) {
        val scan = scala.io.StdIn

        val fw = new PrintWriter(sys.env("OUTPUT_PATH"))

        val nm = scan.readLine.split(" ")

        val n = nm(0).trim.toInt

        val m = nm(1).trim.toInt

        val a = scan.readLine.split(" ").map(_.trim.toInt)

        val b = scan.readLine.split(" ").map(_.trim.toInt)
        val total = getTotalX(a, b)

        fw.println(total)

        fw.close()
    }
}

//the following is other people's solution
object Solution {

    def main(args: Array[String]) {
        val sc = new java.util.Scanner(System.in)
        val n = sc.nextInt
        val m = sc.nextInt
        val a = (1 to n).map(_ => sc.nextInt).sorted
        val b = (1 to m).map(_ => sc.nextInt).sorted
        var r = 0
        for {
            i <- a.last to b.head
        } {
            if (a.forall(x => i % x == 0) && b.forall(x => x % i == 0)) {
                r += 1
            }
        }
        println(r)
    }
}
