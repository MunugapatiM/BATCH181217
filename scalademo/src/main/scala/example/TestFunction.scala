package example

import java.util.Date

object TestFunction {
   def main(args: Array[String]) { 
//        delayed(time()); 
//        printString("AAA","BBB","CCC")
//        println("Default parameters"+addInt());
     
     println(addInt(b=10,a=20));
        
        val today:Date=new Date();
        printMsg(today,"Message1");
        printMsg(today,"Message2");
        printMsg(today,"Message3");
        printMsg(today,"Message4");
        
         val logWithDateBound = printMsg(today, _ : String) 
         logWithDateBound("Message1")
         logWithDateBound("Message2")
         logWithDateBound("Message3")
         logWithDateBound("Message4")
         
         println(apply(layout,5));
         
         val fnOne = (i:Int)=>"i value is "+i;
         
         println(apply("i value is "+_,23))
        println( strcat("ABX")("EDG"));
         println(strcat("HURRAY END OF CLASS")());
   } 
   
  
   
def strcat(s1: String)(s2: String="OPTIONAL") = s1 + s2 

def strcat1(s1: String) = (s2: String) => s1 + s2 


   def time():Long = { 
      println("Getting time in nano seconds") 
      System.nanoTime 
   } 
   def delayed( t: => Long ) = { 
      println("In delayed method") 
      println("Param: " + t) 
   } 
   
   def printString(args:String*):Unit = {
     var i:Int = 0;
     
     for(arg<- args){
       println("args("+i+")="+arg);
       i = i+1;
     }
   }
   
   def addInt( a:Int = 5, b:Int = 7 ) : Int = { 
      var sum:Int = 0 
      sum = a + b 
      return sum 
   } 
   
   def factorial(i:Int) = {
     
     def factorial(x:Int, i:Int):Int = {
       if(i==1) return 1;
       
       factorial(x,i-1)
     }
   }
   
   
   def printMsg(date:Date, msg:String):Unit = {
     println("Logging msg at:"+date+msg);
   }
   
   def factorial(n: BigInt): BigInt = {   
      if (n <= 1) 
         1   
      else     
      n * factorial(n - 1) 
   } 
   
   
   def apply(f: Int => String, v: Int) = f(v) 

   def layout[A](x: A) = "[" + x.toString() + "]" 
   
   
   
   
   
   
   
   
   
} 
