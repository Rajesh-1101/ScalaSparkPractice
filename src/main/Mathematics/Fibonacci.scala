object Fibonacci {
  def fibonacci(n: Int): Int = {
    if(n <= 1)
      n
    else
      fibonacci(n-1) + fibonacci(n-2)
  }

  def fibonacciSeries(n:Int): List[Int] = {
    var a = 0
    var b = 1
    var result = List(a)

    for (_ <- 1 until n){
      result = result :+ b
      val temp = b
      b = a + b
      a = temp
    }
    result
  }
  def main(args: Array[String]):Unit={

    val x = 10
    println(s"The $x-th Fibonacci number is: ${fibonacci(x)}")

    val n = 15 // Change this to the desired number of Fibonacci numbers
    val series = fibonacciSeries(n)
    println(s"The first $n Fibonacci numbers are: ${series.mkString(", ")}")

  }

}
