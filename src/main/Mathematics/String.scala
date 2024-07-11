// Getting familiar with String and basic text literals

// Problem 1: Create a Scala program to reverse, and then format to upper case, the given String:

object String {
  def main(args: Array[String]): Unit = {
    val strToFormat = "scala-exercises"
    val reverseStr = strToFormat.reverse
    val upperCaseStr = strToFormat.toUpperCase()

    println(s"$strToFormat revered is : $reverseStr and upperCase is : $upperCaseStr")
  }
}

// Problem 2: Create a Scala program to output the following basic JSON notation.
/*{
"donut_name":"Vanilla Donut",
"quantity_purchased":"10",
"price":2.5
}
 */
object donut {
  def main(args: Array[String]): Unit = {
    val donutName = "Vanilla Donut"
    val quntityPurchased = 10
    val price = 2.50

    val donutJSON =
      s"""
        |{
        |"donut_name" = $donutName
        |"quntity_purchased" = $quntityPurchased
        |"price" = $price
        |}
        |""".stripMargin

    println(donutJSON)
  }
}

/*
Problem 3: Create a Scala program to prompt customers for their name and age.
The format for the name and age labels should be in bold. And, the name literal
should be underlined.
 */

import scala.io.StdIn.{readInt, readLine}

object Name{
  def capitalizeFirstLetter(s: String): String ={
    s.split(" ").map(_.capitalize).mkString(" ")
}
  def main(args: Array[String]): Unit ={

    val name = capitalizeFirstLetter(readLine("Enter your name: "))
    println("Enter your age: ")
    val age = readInt()

    println(Console.BOLD)
    print("Name: ")
    print(Console.UNDERLINED)
    print(name)
    print(Console.RESET)
    println(Console.BOLD)
    print("Age: ")
    print(Console.RESET)
    print(age)
  }
}


