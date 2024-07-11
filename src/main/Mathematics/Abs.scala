object Abs {
  def abs(x: Int):Int ={
    if(x < 0) -x else x
  }

  def absMax(numbers: List[Int]): Option[Int]={
    numbers match{
      case Nil => None  // Handle empty list
      case _ => Some(numbers.maxBy(math.abs))  //// maximum by absolute value
    }
  }

  def absMin(numbers: List[Int]): Option[Int] = numbers match {
    case Nil => None // Handle empty list case
    case _ =>
      Some(numbers.minBy(math.abs)) // Find the minimum by absolute value
  }


  def main(args: Array[String]): Unit = {
    val num1 = -5
    val num2 = 7
    val list1 = List(-5, 28, -9, 7, 45, -19, -34)
    val list2 = List()
    val result1 = absMax(list2)
    val numbers = List(-10, 5, -7, 8, -3, 2)
    val result2 = absMin(numbers)

    println(s"Absolute value of $num1 is: ${abs(num1)}")
    println(s"Absolute value of $num2 is: ${abs(num2)}")

    result1 match {
      case Some(maxValue) => println(s"The absolute maximum value is: $maxValue")
      case None => println("The list is empty.")
    }

    result2 match {
          case Some(minValue) => println(s"The absolute minimum value is: $minValue")
          case None => println("The list is empty.")
        }
    }
  }

