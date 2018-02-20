package example

import java.io.BufferedReader
import java.io.FileInputStream
import java.io.FileReader
import java.io.FileWriter

import scala.io.Source

object FileUtil {
  val inputFile = "E:/SAIWS/SCALA/ProjectWS/Datasets/people.txt"
  val outputFile = "E:/SAIWS/SCALA/ProjectWS/Datasets/people_output.txt"
  def main(args: Array[String]): Unit = {
    val iLines: List[String] = Source.fromFile(inputFile).getLines().toList;
    //    val iLines2 = iLines;
    iLines.foreach(println)

    println("-----While----");

    for (iLine <- iLines) {
      println(iLine)
    }

    val fw = new FileWriter(outputFile)

    for (iLine <- iLines) {
      fw.write(iLine+"\r\n");
    }
    fw.flush();
    fw.close();

    val inputStream = new FileInputStream(inputFile);
//    val byteArr = new (1024)Array[Byte];
//    while(
 
    println("Reading Data");
    val inputReader = new FileReader(inputFile)
    val inputR = new BufferedReader(inputReader);
    var input:String = null
    while( (input = inputR.readLine())!=null)
    {
     println(input); 
    }
    inputReader.close();
    inputR.close();
    inputStream.close();
    
    
  }
}