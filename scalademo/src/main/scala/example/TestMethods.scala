package example

object TestMethods {
  
  def main(args: Array[String]): Unit = {
//    delayTime(time());
//    printString("Hello","World");
//    printString("AAA","BBB","CCC")
//    println(add());
    
    println(factorial(5))
    
  }
  var tFn = () => System.currentTimeMillis();
  
  def time():Long = {
    println("In time method");
    System.currentTimeMillis();
  }
  
  def delayTime(t: => Long):Unit = {
    println("In Delayed Time");
    println("Delayed units: "+t);
  }
  
  
  def printString(args:String*):Unit = {
    var i =0;
    for(arg <- args){
      println("args("+i+")"+arg);
      i=i+1;
      
    }
    
  }
  
  def add(i:Int=5, j:Int=10):Int = {
    i+j
  }
  
  def factorial(x:Int):Int = {
    
    def factorial(i:Int, a:Int):Int = {
      if(i==1){
        println(i)
        return 1;
      }
      print(i+"*");
      i*factorial(i-1,a)
        
    }
    
    factorial(x,x-1)
    
  }
  
}