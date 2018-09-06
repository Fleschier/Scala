import scala.collection.mutable.ListBuffer

object arrFilter{
	def main(args: Array[String]): Unit = {
		val br = new BufferReader(new InputStreamReader(System.in))
		val x = br.readline().trim.toInt
		val arr = ListBuffer[Int]()

		for(i <- 0 until 10000 if(br.readline() != -1)){
			val s = br.readline
			arr += s.trim.toInt
		}

		println(myFilter(x,arr.toList))

	}

	def myFilter(k: Int, arr:List[Int]): List[Int] = {
		arr.filter(x => x < k)
	}
}