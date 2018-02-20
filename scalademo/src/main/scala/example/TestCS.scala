package example

object TestCS 
{
  def main(args: Array[String]): Unit = {

    if (args.length!=0)
    {
    val input = args(0);
    val iLength = input.length();
    
    for (i<-0 to iLength){
      println(i);
    }
    
    for (arg<-args)
    {
      println(arg);
    }
    
    println("------While-----");
    
    var i = 0;
    while (i<iLength)
    {
      println(s"${input.charAt(i)}\t");
      i=i+1;
    }
    
    }else{
      println("Please input values");
    }
    
    for(arg<-args)
    {
      arg match {
      case "Helloworld" => println(" u entered Helloworld");
      case "Helloall" => println(" u entered Helloall");
      case default => println("Default");
    }
  }
    
  }
}