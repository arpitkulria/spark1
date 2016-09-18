
val lst1 = List(1,2,3)
//val lst2 = Nil
val lst2 = List(5,6,7)
//val lst4 = Nil
val n1 = 10
val n2 = 11

val xy = (lst1, lst2)

val ans: List[(Int, Int)] = xy match {
  case (Nil, Nil) => Nil
  case (x: List[Int], Nil) =>  x.map(elm => (elm, n1))
  case (Nil, y: List[Int]) => y.map(elm => (elm, n2))
  case (x: List[Int] ,y: List[Int]) => y.map(elm => (elm, n1)) ::: y.map(elm => (elm, n2))
}


val x  =(lst2,Nil)
x match {
  case (x, y) => x ::: y
  case (x, _) => x
  case (Nil, _)=> Nil
}