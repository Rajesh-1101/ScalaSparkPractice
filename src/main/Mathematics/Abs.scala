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


  def main(args: Array[String]): Unit = {
    val num1 = -5
    val num2 = 7
    val list1 = List(-5, 28, -9, 7, 45, -19, -34)
    val list2 = List()

    println(s"Absolute value of $num1 is: ${abs(num1)}")
    println(s"Absolute value of $num2 is: ${abs(num2)}")

      case Some(maxValue) => println(s"The absolute maximum value is: $maxValue")
      case None => println("The list is empty.")
    }
    }
  }